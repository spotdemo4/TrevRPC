# An Elixir server and client on loopback, through the module
# protoc-gen-trevrpc-elixir generates from proto/: every kind of call, over
# native QUIC and QMux over TLS, handlers from a module and from funs, the
# schema options, and the names Elixir reserves.
defmodule Fixture.Handler do
  @behaviour Fixture.Rpc.Fixture.V1.Greeter

  @impl true
  def say_hello(%{fail: message}, _call),
    do: {:error, :trevrpc_status.new(:invalid_argument, message)}

  def say_hello(%{name: name}, _call), do: {:ok, %{message: "hello " <> name}}

  @impl true
  def countdown(%{name: name, count: count}, call) do
    for index <- count..1//-1, do: :ok = :trevrpc_call.send(call, %{message: name, index: index})
    :ok
  end

  @impl true
  def collect(call), do: collect(call, [])

  defp collect(call, names) do
    case :trevrpc_call.recv(call) do
      {:ok, %{name: name}} -> collect(call, [name | names])
      :eof -> {:ok, %{message: names |> Enum.reverse() |> Enum.join(",")}}
      {:error, status} -> {:error, status}
    end
  end

  @impl true
  def chat(call) do
    case :trevrpc_call.recv(call) do
      {:ok, request} ->
        :ok = :trevrpc_call.send(call, %{message: describe(request)})
        chat(call)

      :eof ->
        :ok

      {:error, status} ->
        {:error, status}
    end
  end

  # Echoes each request; the trailers say how many arrived, and how many as
  # datagrams, which only a call the client gave a datagram flow sends.
  @impl true
  def telemetry(call), do: telemetry(call, 0)

  defp telemetry(call, received) do
    case :trevrpc_call.recv(call) do
      {:ok, %{name: name}} ->
        :ok = :trevrpc_call.send(call, %{message: name})
        telemetry(call, received + 1)

      :eof ->
        :trevrpc_call.set_trailers(call, %{
          "x-received" => Integer.to_string(received),
          "x-datagrams" => Integer.to_string(:trevrpc_call.datagrams_received(call))
        })

      {:error, status} ->
        {:error, status}
    end
  end

  # Echoes each request; the trailers say how many arrived compressed with
  # the request stream's context, which only a client that the schema let
  # compress the stream uses.
  @impl true
  def compressed(call) do
    case :trevrpc_call.recv(call) do
      {:ok, %{name: name}} ->
        :ok = :trevrpc_call.send(call, %{message: name})
        compressed(call)

      :eof ->
        %{stream: stream} = :trevrpc_call.compressed_received(call)
        :trevrpc_call.set_trailers(call, %{"x-stream" => Integer.to_string(stream)})

      {:error, status} ->
        {:error, status}
    end
  end

  @impl true
  def unquote(:end)(%{name: name}, _call), do: {:ok, %{message: "end " <> name}}

  @impl true
  def receive(%{name: name}, _call), do: {:ok, %{message: "receive " <> name}}

  @impl true
  def channel(%{name: name}, _call), do: {:ok, %{message: "channel " <> name}}

  @impl true
  def options(call), do: collect(call)

  # The fields gpb decoded, the oneof and the imported Timestamp too.
  defp describe(request) do
    choice =
      case request do
        %{choice: {:text, text}} -> "text " <> text
        %{choice: {:id, id}} -> "id #{id}"
        %{} -> "none"
      end

    seconds =
      case request do
        %{at: %{seconds: seconds}} -> "#{seconds}"
        %{} -> "unset"
      end

    "#{request.name} #{choice} #{seconds}"
  end
end

defmodule FixtureTest do
  use ExUnit.Case, async: false

  alias Fixture.Rpc.Fixture.V1.Greeter

  @path "/fixture.v1.Greeter/"

  setup_all do
    {:ok, _} = Application.ensure_all_started(:trevrpc)
    san = {:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}

    %{cert: cert, key: key} =
      :public_key.pkix_test_root_cert(~c"localhost", [
        {:key, {:namedCurve, :secp256r1}},
        {:digest, :sha256},
        {:extensions, [san]}
      ])

    {:ok, server} =
      :trevrpc_server.start(%{
        ip: {127, 0, 0, 1},
        port: 0,
        cert: cert,
        key: key,
        routes: Greeter.routes(Fixture.Handler)
      })

    on_exit(fn -> :trevrpc_server.stop(server) end)
    %{server: server, cacerts: [cert]}
  end

  for transport <- [:quic, :tcp] do
    describe "#{transport}" do
      setup context do
        {:ok, port} =
          case unquote(transport) do
            :quic -> :trevrpc_server.port(context.server)
            :tcp -> :trevrpc_server.tcp_port(context.server)
          end

        {:ok, channel} =
          :trevrpc_channel.connect(
            %{
              host: ~c"localhost",
              port: port,
              tcp_port: port,
              transport: unquote(transport),
              cacerts: context.cacerts
            },
            5000
          )

        on_exit(fn -> :trevrpc_channel.close(channel) end)
        %{channel: channel}
      end

      test "unary", %{channel: channel} do
        assert {:ok, %{message: "hello elixir"}, %{headers: _, trailers: _}} =
                 Greeter.say_hello(channel, %{name: "elixir"})

        assert {:error, %{code: :invalid_argument, message: "no"}} =
                 Greeter.say_hello(channel, %{name: "x", fail: "no"}, %{timeout: 5000})
      end

      test "server stream", %{channel: channel} do
        {:ok, call} = Greeter.countdown(channel, %{name: "n", count: 3})
        assert {:ok, %{index: 3}} = :trevrpc_client.recv(call)
        assert {:ok, %{index: 2}} = :trevrpc_client.recv(call)
        assert {:ok, %{index: 1}} = :trevrpc_client.recv(call)
        assert {:eof, _} = :trevrpc_client.recv(call)
      end

      test "client stream", %{channel: channel} do
        {:ok, call} = Greeter.collect(channel)
        for name <- ["a", "b", "c"], do: :ok = :trevrpc_client.send(call, %{name: name})
        :ok = :trevrpc_client.close_send(call)
        assert {:ok, %{message: "a,b,c"}} = :trevrpc_client.recv(call)
      end

      test "bidi with every kind of field", %{channel: channel} do
        {:ok, call} = Greeter.chat(channel, %{timeout: 5000})

        :ok =
          :trevrpc_client.send(call, %{
            name: "é",
            choice: {:text, "t"},
            at: %{seconds: 7, nanos: 0}
          })

        assert {:ok, %{message: "é text t 7"}} = :trevrpc_client.recv(call)
        :ok = :trevrpc_client.send(call, %{name: "b", choice: {:id, 9}})
        assert {:ok, %{message: "b id 9 unset"}} = :trevrpc_client.recv(call)
        :ok = :trevrpc_client.close_send(call)
        assert {:eof, _} = :trevrpc_client.recv(call)
      end

      # The generated client's options make an unreliable call: its requests
      # travel as datagrams over native QUIC, where some may be lost but none
      # duplicated, and on the stream over QMux.
      test "unreliable", %{channel: channel} do
        {:ok, call} = Greeter.telemetry(channel)
        names = for index <- 1..20, do: Integer.to_string(index)
        for name <- names, do: :ok = :trevrpc_client.send(call, %{name: name})
        :ok = :trevrpc_client.close_send(call)
        {echoed, trailers} = drain(call, [])
        received = String.to_integer(trailers["x-received"])
        datagrams = String.to_integer(trailers["x-datagrams"])

        case unquote(transport) do
          :quic ->
            assert Enum.uniq(echoed) == echoed
            assert echoed -- names == []
            assert received in 1..20
            assert datagrams >= 1

          :tcp ->
            assert echoed == names
            assert {received, datagrams} == {20, 0}
        end
      end

      # The generated client's options let it compress the request stream.
      test "stream-compressed", %{channel: channel} do
        {:ok, call} = Greeter.compressed(channel)
        name = String.duplicate("x", 300)

        for _ <- 1..20 do
          :ok = :trevrpc_client.send(call, %{name: name})
          assert {:ok, %{message: ^name}} = :trevrpc_client.recv(call)
        end

        :ok = :trevrpc_client.close_send(call)
        assert {:eof, %{"x-stream" => stream}} = :trevrpc_client.recv(call)
        assert String.to_integer(stream) >= 1
      end

      test "reserved names", %{channel: channel} do
        assert {:ok, %{message: "end e"}, _} = Greeter.end(channel, %{name: "e"})
        assert {:ok, %{message: "receive r"}, _} = Greeter.receive(channel, %{name: "r"})
        assert {:ok, %{message: "channel c"}, _} = Greeter.channel(channel, %{name: "c"})
        {:ok, call} = Greeter.options(channel, %{})
        :ok = :trevrpc_client.close_send(call)
        assert {:ok, %{message: ""}} = :trevrpc_client.recv(call)
      end
    end
  end

  describe "routes" do
    test "carry the schema" do
      routes = Greeter.routes(Fixture.Handler)
      assert map_size(routes) == 10

      assert %{
               kind: :bidirectional_streaming,
               request_type: "fixture.v1.HelloRequest",
               response_type: "fixture.v1.HelloReply",
               unreliable: %{requests: true, responses: true},
               codec: %{encode: _, decode: _}
             } = route = routes[@path <> "Telemetry"]

      refute Map.has_key?(route, :compression)
      assert %{compression: :stream} = route = routes[@path <> "Compressed"]
      refute Map.has_key?(route, :unreliable)

      assert %{kind: :unary} = route = routes[@path <> "SayHello"]
      refute Map.has_key?(route, :compression)
      refute Map.has_key?(route, :unreliable)
      assert %{kind: :client_streaming} = routes[@path <> "Collect"]
      assert %{kind: :server_streaming} = routes[@path <> "Countdown"]
    end

    test "from funs" do
      unary = fn _request, _call -> {:ok, %{}} end
      routes = Greeter.routes(%{say_hello: unary, end: unary})
      assert Enum.sort(Map.keys(routes)) == [@path <> "End", @path <> "SayHello"]
      assert %{handler: ^unary} = routes[@path <> "End"]

      assert_raise ErlangError, ~r/unknown_method/, fn ->
        Greeter.routes(%{nope: unary})
      end
    end

    test "codecs are gpb's" do
      %{codec: codec} = Greeter.routes(Fixture.Handler)[@path <> "SayHello"]
      bin = :greeter_pb.encode_msg(%{name: "x", count: 2}, :"fixture.v1.HelloRequest")
      assert %{name: "x", count: 2} = codec.decode.(bin)
      assert codec.encode.(%{message: "m"}) |> IO.iodata_to_binary() == <<10, 1, ?m>>
    end
  end

  # The replies of a call, in order, and its trailers.
  defp drain(call, replies) do
    case :trevrpc_client.recv(call) do
      {:ok, %{message: message}} -> drain(call, [message | replies])
      {:eof, trailers} -> {Enum.reverse(replies), trailers}
    end
  end
end
