defmodule Microlsm.Afs do
  @moduledoc false
  # Process-based file descriptor to handle async file io

  alias Microlsm.Fs
  import Record

  defrecord :afd,
    pid: nil,
    ref: nil,
    level: nil,
    preads: nil,
    cap: nil

  @type t :: {
          :afd,
          pid :: pid(),
          ref :: reference(),
          level :: non_neg_integer(),
          preads :: [reference()],
          cap :: pos_integer()
        }

  @opaque reply_ref :: reference()

  @spec open(binary(), [atom()], pos_integer()) :: {:ok, t()} | {:error, reason :: term()}
  def open(filename, modes, cap \\ 8) do
    ref = make_ref()
    owner = self()
    start_ref = make_ref()

    pid =
      spawn_link(fn ->
        case Fs.open(filename, modes) do
          {:ok, fd} ->
            send(owner, {start_ref, :ok})
            loop(fd, ref, owner)

          {:error, _} = error ->
            send(owner, {start_ref, error})
            :done
        end
      end)

    receive do
      {^start_ref, result} ->
        case result do
          :ok ->
            afd = afd(pid: pid, ref: ref, level: 0, preads: [], cap: cap)
            {:ok, afd}

          {:error, _} = error ->
            error
        end
    end
  end

  @spec pwrite(t(), non_neg_integer(), non_neg_integer()) :: t()
  def pwrite(afd, offset, data) do
    enqueue(afd, {:pwrite, offset, data})
  end

  @spec sync(t()) :: t()
  def sync(afd) do
    enqueue(afd, :sync)
  end

  @spec datasync(t()) :: t()
  def datasync(afd) do
    enqueue(afd, :datasync)
  end

  @spec close(t()) :: :ok
  def close(afd) do
    afd(level: level, ref: ref) = enqueue(afd, :close)
    await_level(level, ref)
  end

  @spec pread(t(), non_neg_integer(), non_neg_integer()) :: {reply_ref(), t()}
  def pread(afd(pid: pid, preads: preads) = afd, offset, size) do
    reply_ref = make_ref()
    message = {:pread, reply_ref, offset, size}
    send(pid, message)
    afd = afd(afd, preads: [reply_ref | preads])
    {reply_ref, afd}
  end

  @spec receive_pread(t(), reply_ref(), timeout()) :: {:timeout | term(), t()}
  def receive_pread(afd(preads: preads) = afd, reply_ref, timeout \\ :infinity) do
    receive do
      {:pread, ^reply_ref, result} ->
        preads = List.delete(preads, reply_ref)
        {result, afd(afd, preads: preads)}

      after timeout ->
        {:timeout, afd}
    end
  end

  defp loop(fd, ref, owner) do
    receive do
      message ->
        case message do
          {:pwrite, offset, data} ->
            :ok = Fs.pwrite(fd, offset, data)
            send(owner, ref)
            loop(fd, ref, owner)

          :sync ->
            :ok = Fs.sync(fd)
            send(owner, ref)
            loop(fd, ref, owner)

          :datasync ->
            :ok = Fs.datasync(fd)
            send(owner, ref)
            loop(fd, ref, owner)

          {:pread, reply_ref, offset, amount} ->
            result = Fs.pread(fd, offset, amount)
            send(owner, {:pread, reply_ref, result})
            loop(fd, ref, owner)

          :close ->
            :ok = Fs.close(fd)
            send(owner, ref)
            :ok
        end
    end
  end

  defp enqueue(afd(level: level, cap: cap, pid: pid) = afd, message) when level < cap do
    send(pid, message)
    afd(afd, level: level + 1)
  end

  defp enqueue(afd(ref: ref, level: level, cap: cap) = afd, message) when level == cap do
    receive do
      ^ref -> enqueue(afd(afd, level: level - 1), message)
    end
  end

  defp await_level(0, _ref) do
    :ok
  end

  defp await_level(level, ref) do
    receive do
      ^ref -> await_level(level - 1, ref)
    end
  end
end
