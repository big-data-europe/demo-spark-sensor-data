defmodule Dispatcher do

  use Plug.Router

  def start(_argv) do
    port = 80
    IO.puts "Starting Plug with Cowboy on port #{port}"
    Plug.Adapters.Cowboy.http __MODULE__, [], port: port
    :timer.sleep(:infinity)
  end

  plug Plug.Logger
  plug :match
  plug :dispatch

  match "/pipelines/*path" do
    Proxy.forward conn, path, "http://pipeline/pipelines/"
  end

  match "/steps/*path" do
    Proxy.forward conn, path, "http://pipeline/steps/"
  end

  match "/init-daemon/*path" do
    Proxy.forward conn, path, "http://initDaemon/"
  end
	
  match _ do
    send_resp( conn, 404, "Route not found" )
  end

end
