using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ben.Collections.Specialized;

namespace IMDBParser
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            if (!File.Exists("title.basics.tsv.gz"))
            {
                var client = new WebClient();
                await client.DownloadFileTaskAsync("https://datasets.imdbws.com/title.basics.tsv.gz",
                    "title.basics.tsv.gz");
            }

            var parser = new Parser(Environment.ProcessorCount / 2);

            using var fs = File.OpenRead("title.basics.tsv.gz");
            using var gzip = new GZipStream(fs, CompressionMode.Decompress);

            var sw = Stopwatch.StartNew();
            var result = (await parser.Parse(gzip)).ToArray();
            sw.Stop();

            Console.WriteLine("Total milliseconds taken: " + sw.ElapsedMilliseconds);
        }
    }

    internal class Title
    {
        public virtual string TConst { get; set; }
        public virtual string TitleType { get; set; }
        public virtual string PrimaryTitle { get; set; }
        public virtual string OriginalTitle { get; set; }
        public virtual string IsAdult { get; set; }
        public virtual string StartYear { get; set; }
        public virtual string EndYear { get; set; }
        public virtual string RuntimeMinutes { get; set; }
        public virtual string Genres { get; set; }
    }

    internal class Parser
    {
        private readonly int _concurrentCount;
        private Memory<byte> _buffer;
        private volatile LinkedTitle _first;
        private volatile LinkedTitle _last;

        private int _totalCount;
        private readonly Channel<Range> _channel;
        private readonly CountdownEvent _countdownEvent;

        public Parser(int concurrentCount)
        {
            _concurrentCount = concurrentCount;
            _channel = Channel.CreateUnbounded<Range>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = true });
            _countdownEvent = new CountdownEvent(_concurrentCount);
            for (var i = 0; i < _concurrentCount; i++)
            {
                ThreadPool.QueueUserWorkItem(
                    async x =>
                    {
                        await ProcessBuffer();
                        _countdownEvent.Signal();
                    });
            }
        }

        public ValueTask<IEnumerable<Title>> Parse(Stream stream)
        {
            _buffer = new byte[674511603];

            var bytesInBuffer = 0;
            var fillSpan = _buffer.Span;

            var chunkSize = (int)Math.Ceiling(_buffer.Length / (double)_concurrentCount);
            var currentStart = 0;
            var seenNewLine = false;
            var currentChunkSize = 0;
            var writer = _channel.Writer;

            while (true)
            {
                //Fill our working buffer with data
                var bytesRead = stream.Read(fillSpan.Slice(bytesInBuffer, Math.Min(chunkSize - currentChunkSize, _buffer.Length - bytesInBuffer)));
                if (bytesRead == 0)
                {
                    if (currentChunkSize > 0)
                    {
                        var lineEnd = _buffer.Span.Slice(currentStart).LastIndexOf((byte)'\n');
                        writer.TryWrite(new Range(currentStart, currentStart + lineEnd + 1));
                    }

                    break;
                }

                currentChunkSize += bytesRead;

                if (!seenNewLine)
                {
                    var workingSpan = _buffer.Span.Slice(bytesInBuffer, bytesRead);
                    var newLineIndex = workingSpan.IndexOf((byte)'\n') + 1;
                    if (newLineIndex >= 0)
                    {
                        seenNewLine = true;
                        currentStart = newLineIndex;
                        currentChunkSize -= newLineIndex;
                    }
                    else
                    {
                        bytesInBuffer += bytesRead;
                        continue;
                    }
                }


                if (currentChunkSize >= chunkSize)
                {
                    var lineEnd = _buffer.Span.Slice(currentStart, chunkSize).LastIndexOf((byte)'\n');
                    writer.TryWrite(new Range(currentStart, currentStart + lineEnd + 1));
                    currentChunkSize = currentChunkSize - (lineEnd + 1);
                    currentStart += lineEnd + 1;
                }


                bytesInBuffer += bytesRead;
            }

            writer.Complete();

            _countdownEvent.Wait();

            return new ValueTask<IEnumerable<Title>>(new ResultListProvider(_totalCount, _first));
        }

        private async ValueTask ProcessBuffer()
        {
            var reader = _channel.Reader;

            while ((await reader.WaitToReadAsync().ConfigureAwait(false)))
            {
                if (!reader.TryRead(out var range))
                    continue;

                ProcessBuffer(range);
            }
        }

        private void ProcessBuffer(Range range)
        {
            var activeColumn = 0;
            LinkedTitle active = null;
            var bufferPos = range.Start.Value;
            var totalCount = 0;
            LinkedTitle last = null;
            LinkedTitle first = null;

            var workingSpan = _buffer.Span.Slice(range.Start.Value, range.End.Value - range.Start.Value);


            while (true)
            {
                var valueEndIndex = workingSpan.IndexOfAny((byte)'\t', (byte)'\n');
                if (valueEndIndex < 0)
                {
                    Debug.Assert(workingSpan.IsEmpty);
                    break;
                }

                switch (activeColumn++)
                {
                    case 0:
                        active = new LinkedTitle
                        {
                            TConstMem = _buffer.Slice(bufferPos, valueEndIndex)
                        };
                        first ??= active;
                        if (last == null)
                        {
                            last = active;
                        }
                        else
                        {
                            last.Next = active;
                            last = active;
                        }

                        break;
                    case 1:
                        active.TitleTypeMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 2:
                        active.PrimaryTitleMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 3:
                        active.OriginalTitleMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 4:
                        active.IsAdultMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 5:
                        active.StartYearMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 6:
                        active.EndYearMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 7:
                        active.RuntimeMinutesMem = _buffer.Slice(bufferPos, valueEndIndex);
                        break;
                    case 8:
                        active.GenresMem = _buffer.Slice(bufferPos, valueEndIndex);
                        activeColumn = 0;
                        totalCount++;
                        break;
                }

                workingSpan = workingSpan.Slice(valueEndIndex + 1);
                bufferPos += valueEndIndex + 1;
            }

            Debug.Assert(activeColumn == 0);

            var update = false;
            if (_first == null)
            {
                lock (this)
                {
                    if (_first == null)
                    {
                        _first = first;
                        _last = last;
                    }
                    else
                    {
                        update = true;
                    }
                }
            }
            else
            {
                update = true;
            }

            while (update)
            {
                if (Interlocked.CompareExchange(ref _last.Next, first, null) == null)
                {
                    _last = last;
                    break;
                }
            }

            Interlocked.Add(ref _totalCount, totalCount);
        }

        private class LinkedTitle : Title
        {
            private static readonly InternPool InternPool = new();
            private string _const;
            private string _endYear;
            private string _genres;
            private string _isAdult;
            private string _originalTitle;
            private string _primaryTitle;
            private string _runtimeMinutes;
            private string _startYear;
            private string _titleType;
            internal Memory<byte> EndYearMem;
            internal Memory<byte> GenresMem;
            internal Memory<byte> IsAdultMem;
            internal LinkedTitle Next;
            internal Memory<byte> OriginalTitleMem;
            internal Memory<byte> PrimaryTitleMem;
            internal Memory<byte> RuntimeMinutesMem;
            internal Memory<byte> StartYearMem;

            internal Memory<byte> TConstMem;
            internal Memory<byte> TitleTypeMem;

            public override string TConst
            {
                get
                {
                    if (_const != null)
                        return _const;

                    return _const = ConvertToString(TConstMem);
                }
                set => _const = value;
            }

            public override string TitleType
            {
                get
                {
                    if (_titleType != null)
                        return _titleType;

                    return _titleType = ConvertToString(TitleTypeMem);
                }
                set => _titleType = value;
            }

            public override string PrimaryTitle
            {
                get
                {
                    if (_primaryTitle != null)
                        return _primaryTitle;

                    return _primaryTitle = ConvertToString(PrimaryTitleMem);
                }
                set => _primaryTitle = value;
            }

            public override string OriginalTitle
            {
                get
                {
                    if (_originalTitle != null)
                        return _originalTitle;

                    return _originalTitle = ConvertToString(OriginalTitleMem);
                }
                set => _originalTitle = value;
            }

            public override string IsAdult
            {
                get
                {
                    if (_isAdult != null)
                        return _isAdult;

                    return _isAdult = ConvertToString(IsAdultMem);
                }
                set => _isAdult = value;
            }

            public override string StartYear
            {
                get
                {
                    if (_startYear != null)
                        return _startYear;

                    return _startYear = ConvertToString(StartYearMem);
                }
                set => _startYear = value;
            }

            public override string EndYear
            {
                get
                {
                    if (_endYear != null)
                        return _endYear;

                    return _endYear = ConvertToString(EndYearMem);
                }
                set => _endYear = value;
            }

            public override string RuntimeMinutes
            {
                get
                {
                    if (_runtimeMinutes != null)
                        return _runtimeMinutes;

                    return _runtimeMinutes = ConvertToString(RuntimeMinutesMem);
                }
                set => _runtimeMinutes = value;
            }

            public override string Genres
            {
                get
                {
                    if (_genres != null)
                        return _genres;

                    return _genres = ConvertToString(GenresMem);
                }
                set => _genres = value;
            }

            private string ConvertToString(Memory<byte> memory)
            {
                Span<char> valueBuffer = stackalloc char[memory.Length * sizeof(char)];
                Utf8.ToUtf16(memory.Span, valueBuffer, out _, out var size);

                return InternPool.Intern(valueBuffer.Slice(0, size));
            }
        }

        private class ResultListProvider : ICollection<Title>
        {
            private readonly LinkedTitle _first;

            public ResultListProvider(int count, LinkedTitle first)
            {
                _first = first;
                Count = count;
            }

            public IEnumerator<Title> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(Title item)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(Title item)
            {
                throw new NotImplementedException();
            }

            public void CopyTo(Title[] array, int arrayIndex)
            {
                var node = _first;
                int i;
                for (i = arrayIndex; i < array.Length && node != null; i++, node = node.Next)
                    array[i] = node;
            }

            public bool Remove(Title item)
            {
                throw new NotImplementedException();
            }

            public int Count { get; }

            public bool IsReadOnly => true;
        }
    }
}