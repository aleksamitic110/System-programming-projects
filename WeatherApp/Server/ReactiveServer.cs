using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Diagnostics;
using WeatherApp.Models;
using WeatherApp.Services;

namespace WeatherApp.Server
{

	public class ReactiveServer
	{

		private readonly HttpListener _listener; // standardni listener
		private readonly Subject<HttpListenerContext> _requestStream; // klijent -> server -> stream ka OpenMateo api

		public ReactiveServer(string prefix)
		{
			_listener = new HttpListener();
			_listener.Prefixes.Add(prefix);
			_requestStream = new Subject<HttpListenerContext>();
		}

		public IObservable<HttpListenerContext> RequestStream => _requestStream; // Stavjamo da pipeline bude vidljiv svima ali samo server moze da radi OnNext(context)

		public async Task StartAsync()
		{
			_listener.Start();
			Console.WriteLine("Server started... Listening for requests at:");
            foreach (var prefix in _listener.Prefixes)
            {
                Console.WriteLine(prefix);
            }

            while (true)
			{
				var context = await _listener.GetContextAsync();
				_requestStream.OnNext(context); // salje context server -> OpenMateoApi
			}
		}
	}
}
