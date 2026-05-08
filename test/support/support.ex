defmodule Microlsm.Test.Support do
  def setup_datadir do
    unique = :erlang.unique_integer([:positive])
    name = :"microlsm_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "microlsm_test_#{unique}")
    File.rm_rf(data_dir)
    File.mkdir(data_dir)

    %{name: name, data_dir: data_dir}
  end

  ## RPS

  def start_rps do
    Process.put(:__reqs_count, 0)
    Process.put(:__reqs_ts, now())
  end

  def print_rps do
    reqs_count = Process.get(:__reqs_count)
    ts = Process.get(:__reqs_ts)
    now = now()

    if ts != now do
      approx = Float.ceil(reqs_count / ((now - ts) / 1_000), 2)
      IO.puts "RPS #{approx}"
    end
  end

  def bump_rps do
    reqs_count = Process.get(:__reqs_count)
    ts = Process.get(:__reqs_ts)
    now = now()

    if now - ts >= 1_000 do
      approx = Float.ceil((reqs_count + 1) / ((now - ts) / 1_000), 2)
      IO.puts "RPS #{approx}"
      Process.put(:__reqs_count, 0)
      Process.put(:__reqs_ts, now)
    else
      Process.put(:__reqs_count, reqs_count + 1)
    end
  end

  defp now do
    :erlang.monotonic_time(:millisecond)
  end

  ## Watcher

  def start_watcher(name) do
    owner = self()
    spawn(fn -> watcher_loop(owner, name) end)
  end

  defp watcher_loop(owner, name) do
    pid = Process.whereis(name)
    IO.inspect Process.info(pid, :current_stacktrace)
    {:links, links} = Process.info(pid, :links)

    for link <- links, link != owner do
      IO.inspect Process.info(link, :current_stacktrace), label: inspect(link)
    end

    Process.sleep(500)
    watcher_loop(owner, name)
  end
end

