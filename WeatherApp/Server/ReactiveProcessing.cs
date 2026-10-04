using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using WeatherApp.Models;
using WeatherApp.Services;

namespace WeatherApp.Server
{
	
	public static class ReactiveProcessing
	{
		public static void SetupPipeline(ReactiveServer server, OpenMeteoService meteoService)
		{
			// Glavni pipeline za obradu HTTP zahteva
			var pipeline = server.RequestStream
				.ObserveOn(TaskPoolScheduler.Default) // paralelna obrada
				.SelectMany(ctx => Observable.FromAsync(() => HandleRequest(ctx, meteoService))
											 .Retry(3)
											 .Catch<LogEntry, Exception>(ex =>
											 {
												 Console.WriteLine($"[PIPELINE ERROR] {ex.Message}");
												 return Observable.Empty<LogEntry>();
											 }))
				.Publish()
				.RefCount();


			Thresholds thresholds = new Thresholds(10, 20, 30, 40);

			// Svi uspesni zahtevi
			pipeline.Where(log => log.Success)
					.Subscribe(log =>
					{
						Console.WriteLine($"[SUCCESS] {log.Method} {log.RequestUrl} from {log.ClientIP}");
					});

			//Alert za visok CO > 100
			pipeline.Where(log => log.AirQuality?.CO > thresholds.CO_MAX)
					.Subscribe(log =>
					{
						Console.WriteLine($"HIGH CO ALERT! {log.AirQuality?.CO ?? 0} µg/m3 at {log.Timestamp}");
					});

			//Alert za visok NO2 > 200
			pipeline.Where(log => log.AirQuality?.NO2 > thresholds.NO2_MAX)
					.Subscribe(log =>
					{
						Console.WriteLine($"HIGH NO2 ALERT! {log.AirQuality?.NO2 ?? 0} µg/m3 at {log.Timestamp}");
					});

			//Agregacija prosecnih vrednosti na po 5 minuta
			pipeline.Where(log => log.AirQuality != null)
					.Buffer(TimeSpan.FromMinutes(5))
					.Where(buf => buf.Count > 0)
					.Subscribe(buf =>
					{
						var avgPM10 = buf.Average(l => l.AirQuality?.PM10);
						var avgPM25 = buf.Average(l => l.AirQuality?.PM25);
						var avgCO = buf.Average(l => l.AirQuality?.CO);
						var avgNO2 = buf.Average(l => l.AirQuality?.NO2);

						Console.WriteLine($"[AGGREGATE 5min] Avg PM10={avgPM10:F2}, PM2.5={avgPM25:F2}, CO={avgCO:F2}, NO2={avgNO2:F2}");
					});
		}

		private static async Task<LogEntry> HandleRequest(HttpListenerContext context, OpenMeteoService meteoService)
		{
			var sw = System.Diagnostics.Stopwatch.StartNew();

			string url = context.Request.RawUrl ?? "/";
			string method = context.Request.HttpMethod;

			string clientIP = context.Request.RemoteEndPoint?.Address.ToString() ?? "unknown";
			if (clientIP == "::1") clientIP = "127.0.0.1";

			var logEntry = new LogEntry
			{
				Timestamp = DateTime.Now,
				RequestUrl = url,
				Method = method,
				ClientIP = clientIP,
				Success = false
			};

			try
			{
				if (url == "/favicon.ico")
				{
					context.Response.StatusCode = 204;
					context.Response.Close();
					logEntry.Success = true;
					logEntry.StatusCode = 204;
					return logEntry;
				}

				// Preuzimanje query parametara
				double latitude = 0, longitude = 0;

				if (context.Request.Url != null) // cisto da se resimo warninga za moguci null
				{
					var queryParams = System.Web.HttpUtility.ParseQueryString(context.Request.Url.Query);

					double.TryParse(queryParams["lat"], out latitude);
					double.TryParse(queryParams["lon"], out longitude);

					logEntry.Latitude = latitude;
					logEntry.Longitude = longitude;

					// Poziv OpenMeteo API-ja
					AirQualityData? airQuality = await meteoService.GetAirQualityAsync(latitude, longitude);
					logEntry.AirQuality = airQuality;

					string responseString;
					if (airQuality != null)
					{
						responseString = System.Text.Json.JsonSerializer.Serialize(airQuality);
						context.Response.ContentType = "application/json";
						logEntry.Success = true;
						logEntry.StatusCode = 200;
					}
					else
					{
						responseString = "{\"error\":\"Unable to fetch air quality\"}";
						context.Response.ContentType = "application/json";
						logEntry.Success = false;
						logEntry.StatusCode = 502;
					}

					byte[] buffer = Encoding.UTF8.GetBytes(responseString);
					context.Response.ContentLength64 = buffer.Length;
					await context.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
					context.Response.OutputStream.Close();

					await Logger.LogRequestAsync($"{method} {url} from {clientIP}");
				}
			}
			catch (Exception ex)
			{
				logEntry.Success = false;
				logEntry.ErrorMessage = ex.Message;
				logEntry.StatusCode = 500;
				await Logger.LogErrorAsync($"Error handling request: {ex.Message}");
			}
			finally
			{
				sw.Stop();
				logEntry.ElapsedMs = sw.ElapsedMilliseconds;
			}

			return logEntry;
		}
	}
}
