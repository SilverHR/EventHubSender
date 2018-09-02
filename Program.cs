using System;
using System.Text;
using System.Threading;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace EventHubSender
{
    static class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "[event hub namespace connection string]";
        private const string EhEntityPath = "[event hub name]";
        private static Random random;
        private static double t = 1;

        static double Rand()
        {
            return random.NextDouble();
        }

        static void Main(string[] args)
        {
            random = new Random();
            var interval = 5000;
            double runtime = 0;

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString) { EntityPath = EhEntityPath };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            var timer = new Timer(_ => Action(interval, ref runtime), null, 0, interval);
            Thread.Sleep(TimeSpan.FromMinutes(30));
        }

        static void Action(int interval, ref double runtime)
        {
            var d = DateTime.Now;
            var us = 308;
            var a = us + (d.Second % 3);

            for (var m = 1; m <= (int)Math.Floor((Math.Sqrt(t / 60000)) + 1); m++)
            {
                var p = (int)Math.Floor(Rand() * ((((m % 3) + 1) * 10) - ((m % 3) + 1) + 1) + ((m % 3) + 1));
                for (var i = us; i <= a; i++)
                {
                    Console.WriteLine("Receiving data...");
                    var sa = ((i % 3) + 1) * Math.Sign(Rand() * (2) - 3);
                    if (sa == 0)
                    {
                        sa = -1;
                    }
                    var data = JsonConvert.SerializeObject(new { Date = d, StoreID = i, ProductID = p, StockAdjust = sa });
                    var eventData = new EventData(Encoding.UTF8.GetBytes(data));
                    eventHubClient.SendAsync(eventData);
                }
            }
            t = t + interval;
            runtime = t / 60000;
            Console.WriteLine(runtime.ToString("0.00") + " minutes elapsed.");
        }
    }
}
