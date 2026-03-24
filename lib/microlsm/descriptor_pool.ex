defmodule Microlsm.DescriptorPool do
  # Round-robin checkout pool with atomics compare_exchange for counter
  # and ets select_replace for locking which lazily opens the file descriptors in
  # isolated cell processes

  # This whole module exists because OTP lacks the
  # ability to safely share fds among several processes

  # Use Process.link and some supervisor for cells
  @moduledoc false

  @opaque t :: {term(), :atomics.atomics_ref(), pos_integer(), binary()}

  @opaque pool :: {:ets.table(), pid()}

  bypass? = false

  import Microlsm.Debug

  alias Microlsm.Fs

  @spec new() :: pool()
  def new do
    table = :ets.new(__MODULE__, [:set, :public])
    {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {table, sup}
  end

  @spec delete(pool()) :: :ok
  def delete({table, sup}) do
    DynamicSupervisor.stop(sup)
    true = :ets.delete(table)
    :ok
  end

  @spec clean(pool()) :: :ok
  def clean(pool) do
    {table, sup} = pool
    :ets.delete_all_objects(table)

    for entry <- DynamicSupervisor.which_children(sup) do
      {:undefined, pid, :worker, _} = entry
      true = is_pid(pid)
      :ok = DynamicSupervisor.terminate_child(sup, pid)
    end

    :ok
  end

  if bypass? do
    def add(_pool, filename, _count) do
      filename
    end

    def remove(_pool, _state) do
      :ok
    end

    def checkout(_pool, filename, func, _timeout \\ nil) do
      {:ok, fd} = Fs.open(filename, [:read])
      try do
        func.(fd)
      after
        Fs.close(fd)
      end
    end
  else
    @spec add(pool(), filename :: String.t(), pos_integer()) :: t()
    def add(_pool, filename, count) do
      filename_ref = :erlang.unique_integer()
      atomics_ref = :atomics.new(1, signed: false)

      {filename_ref, atomics_ref, count, filename}
    end

    @spec remove(pool(), t()) :: :ok
    def remove(pool, state) do
      {table, _sup} = pool
      {filename_ref, _atomics_ref, _count, _filename} = state
      pattern = {{filename_ref, :_}, :_, :_}
      :ets.select_delete(table, [{pattern, [], [true]}])
      :ok
    end

    @spec checkout(pool(), t(), (tuple() -> result), timeout()) :: result
    when result: term()
    def checkout(pool, state, func, timeout \\ 5_000) do
      {table, sup} = pool
      {filename_ref, atomics_ref, count, filename} = state
      {_index, cell} = checkout_cell(sup, table, filename_ref, filename, atomics_ref, count)
      dlog({:checked_out, filename_ref, index, filename, cell})
      monitor_ref = Process.monitor(cell, alias: :reply_demonitor)
      send(cell, {:execute, monitor_ref, func, self()})
      receive do
        {^monitor_ref, :ok, result} ->
          result

        {^monitor_ref, kind, error, stacktrace} ->
          {:current_stacktrace, current_stacktrace} = Process.info(self(), :current_stacktrace)
          [_process_info_call | current_stacktrace] = current_stacktrace
          :erlang.raise(kind, error, stacktrace ++ current_stacktrace)

        {:DOWN, ^monitor_ref, :process, ^cell, reason} ->
          raise "Cell exited with #{inspect(reason)}"

        after timeout ->
          Process.demonitor(monitor_ref, [:flush])
          raise "Timeout"
      end
    end
  end

  ## Cell

  defp loop_cell(fd, filename_ref, table, index) do
    receive do
      {:execute, ref, func, owner} ->
        try do
          func.(fd)
        else
          result ->
            send(ref, {ref, :ok, result})
        catch
          kind, error ->
            send(ref, {ref, kind, error, __STACKTRACE__})
        after
          checkin_cell(owner, table, filename_ref, index)
        end

        loop_cell(fd, filename_ref, table, index)

      _ ->
        loop_cell(fd, filename_ref, table, index)

      after 5_000 ->
        # Recovery for a case when a caller dies during
        # checkout right after checkout but before sending a message

        with(
          [{{^filename_ref, ^index}, owner, _}] when is_pid(owner) <- :ets.lookup(table, {filename_ref, index}),
          false <- Process.alive?(owner)
        ) do
          ms = [{{{filename_ref, index}, owner, :"$1"}, [], [{{{{filename_ref, index}}, :free, :"$1"}}]}]
          1 = :ets.select_replace(table, ms)
        end

        loop_cell(fd, filename_ref, table, index)
    end
  end

  ## Checkout

  defp checkin_cell(owner, table, filename_ref, index) do
    ms = [{{{filename_ref, index}, owner, :"$1"}, [], [{{{{filename_ref, index}}, :free, :"$1"}}]}]
    1 = :ets.select_replace(table, ms)
  end

  defp start_cell(sup, table, filename_ref, index, filename, as) do
    spec = %{
      id: {filename_ref, index},
      start: {__MODULE__, :do_start_cell, [table, filename_ref, index, filename, as]},
      restart: :transient
    }

    {:ok, cell} = DynamicSupervisor.start_child(sup, spec)
    cell
  end

  def do_start_cell(table, filename_ref, index, filename, as) do
    ref = make_ref()
    owner = self()

    cell =
      spawn_link(fn ->
        Process.flag(:priority, :high)
        {:ok, fd} = Fs.open(filename, [:read])
        ms = [{{{filename_ref, index}, :_, :_}, [], [{{{{filename_ref, index}}, as, self()}}]}]
        1 = :ets.select_replace(table, ms)
        send(owner, ref)
        loop_cell(fd, filename_ref, table, index)
      end)

    receive do
      ^ref -> {:ok, cell}
    end
  end

  defp checkout_cell(sup, table, filename_ref, filename, atomics_ref, count) do
    value = :atomics.add_get(atomics_ref, 1, 1)
    index = rem(value, count)

    do_checkout_cell(sup, table, filename_ref, filename, atomics_ref, count, index, index)
  end

  defp do_checkout_cell(sup, table, filename_ref, filename, atomics_ref, count, start_index, index) do
    owner = self()

    case :ets.insert_new(table, {{filename_ref, index}, self(), nil}) do
      false ->
        # Ex2ms.fun do {{filename_ref, i}, :free, cell} -> {{filename_ref, i}, self(), cell} end
        ms = [{{{filename_ref, index}, :free, :"$1"}, [], [{{{{filename_ref, index}}, self(), :"$1"}}]}]
        case :ets.select_replace(table, ms) do
          0 ->
            next_value = :atomics.add_get(atomics_ref, 1, 1)
            next_index = rem(next_value, count)
            dlog(:dp_cnt)
            do_checkout_cell(sup, table, filename_ref, filename, atomics_ref, count, start_index, next_index)

          1 ->
            [{{^filename_ref, ^index}, ^owner, cell}] = :ets.lookup(table, {filename_ref, index})
            {index, cell}
        end

      true ->
        cell = start_cell(sup, table, filename_ref, index, filename, self())
        dlog(:lazy_descriptor_init)
        {index, cell}
    end
  end
end
