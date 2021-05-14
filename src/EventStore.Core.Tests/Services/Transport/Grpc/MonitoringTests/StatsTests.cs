using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Client.Monitoring;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Integration;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Grpc.MonitoringTests {
	public class StatsTests {
		public class when_reading_stats : specification_with_cluster {
			
			private const int _expected = 3;
			private const int _refreshTimePeriodInMs = 250;
			private readonly List<StatsResp> _stats;

			public when_reading_stats() {
				_stats = new List<StatsResp>();
			}
			
			protected override async Task Given() {
				var node = GetLeader();
				await Task.WhenAll(node.AdminUserCreated, node.Started);
				
				using var channel = GrpcChannel.ForAddress(new Uri($"https://{node.HttpEndPoint}"),
					new GrpcChannelOptions {
						HttpClient = new HttpClient(new SocketsHttpHandler {
							SslOptions = {
								RemoteCertificateValidationCallback = delegate { return true; }
							}
						}, true)
					});
				var client = new Client.Monitoring.Monitoring.MonitoringClient(channel);
				var request = new StatsReq {
					RefreshTimePeriodInMs = _refreshTimePeriodInMs
				};
				
				using var resp = client.Stats(request);
			
				var count = 0;
				var cts = new CancellationTokenSource(_refreshTimePeriodInMs * _expected * 2);
				while (count < _expected && await resp.ResponseStream.MoveNext(cts.Token)) {
					_stats.Add(resp.ResponseStream.Current);
					count++;
				}
			}

			[Test]
			public void should_receive_expected_amount_of_stats_messages() {
				Assert.AreEqual(_expected, _stats.Count);
			}

		}	
	}
}
