defmodule Microlsm.DescriptorPool do
  @behaviour NimblePool

  ## Public

  def with_descriptor(pool, func, timeout \\ 5_000) do
    callback =
      fn _from, fd ->
        result = func.(fd)
        {result, fd}
      end

    NimblePool.checkout!(pool, :checkout, callback, timeout)
  end

  def start_link(opts) do
    count = Keyword.fetch!(opts, :count)
    NimblePool.start_link(worker: {__MODULE__, opts}, pool_size: count)
  end

  ## Callbacks

  def init_pool(opts) do
    filename = Keyword.fetch!(opts, :filename)
    mods = Keyword.get(opts, :mods, [:read])

    state = %{
      filename: filename,
      mods: mods
    }

    {:ok, state}
  end

  def init_worker(state) do
    %{filename: filename, mods: mods} = state
    {:ok, fd} = :prim_file.open(filename, mods)
    IO.inspect :prim_file.get_handle(fd), label: :init_worker
    {:ok, fd, state}
  end

  def handle_checkout(:checkout, {pid, _}, fd, pool_state) do
    IO.inspect pid, label: :checkout_from
    {:file_descriptor, :prim_file, meta} = fd
    client_fd = {:file_descriptor, :prim_file, %{meta | owner: pid}}
    {:ok, client_fd, {fd, pid}, pool_state}
  end

  def handle_checkin(client_fd, {pid, _}, _nil, pool_state) do
    IO.inspect pid, label: :checkin_from
    {:ok, client_fd, pool_state}
  end

  def terminate_worker(reason, {fd, pid}, pool_state) do
    IO.inspect {reason, pid, fd}, label: :terminating
    IO.inspect :prim_file.get_handle(fd)
    {:ok, pool_state}
  end
end

