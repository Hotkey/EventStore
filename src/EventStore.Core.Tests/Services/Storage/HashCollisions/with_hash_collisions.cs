using System;
using EventStore.ClientAPI;
using EventStore.Core.Tests;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.Services.Storage.ReaderIndex;

namespace EventStore.Core.Services.Storage.HashCollisions
{
    [TestFixture]
    public class HashCollisionTestFixture : SpecificationWithDirectoryPerTestFixture{
        protected int _hashCollisionReadLimit = 5;
        protected TableIndex _tableIndex;
        protected IIndexReader _indexReader;
        private IIndexBackend _indexBackend;
        private IHasher _lowHasher;
        private IHasher _highHasher;
        private string _indexDir;
        private TFReaderLease _fakeReader;

        protected virtual void given(){}
        protected virtual void when(){}

        [TestFixtureSetUp]
        public void Setup(){
            given();
            _indexDir = PathName;
            _fakeReader = new TFReaderLease(new FakeReader());
            _indexBackend = new FakeIndexBackend(_fakeReader);
            _lowHasher = new XXHashUnsafe();
            _highHasher = new Murmur3AUnsafe();
            _tableIndex = new TableIndex(_indexDir, _lowHasher, _highHasher,
                                         () => new HashListMemTable(PTableVersions.Index32Bit, maxSize: 5),
                                         () => _fakeReader,
                                         PTableVersions.Index32Bit,
                                         maxSizeForMemory: 5,
                                         maxTablesPerLevel: 2);
            _tableIndex.Initialize(long.MaxValue);
            _indexReader = new IndexReader(_indexBackend, _tableIndex, new EventStore.Core.Data.StreamMetadata(), _hashCollisionReadLimit);

            when();
            //wait for the mem table to be dumped
            System.Threading.Thread.Sleep(500);
        }
    }

    [TestFixture]
    public class when_stream_does_not_exist : HashCollisionTestFixture{
        protected override void given(){
            _hashCollisionReadLimit = 5;
        }
        protected override void when(){
            //mem table
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 0, 3);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 1, 5);
        }
        [Test]
        public void should_return_no_stream(){
            Assert.AreEqual(ExpectedVersion.NoStream, _indexReader.GetStreamLastEventNumber("account--696193173"));
        }
    }

    [TestFixture]
    public class when_stream_is_out_of_range_of_read_limit : HashCollisionTestFixture{
        protected override void given(){
            _hashCollisionReadLimit = 1;
        }
        protected override void when(){
            //ptable 1
            _tableIndex.Add(1, "account--696193173", 0, 0);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 0, 3);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 1, 5);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 2, 7);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 3, 9);
            //mem table
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 4, 13);
        }
        [Test]
        public void should_return_invalid_event_number(){
            Assert.AreEqual(EventStore.Core.Data.EventNumber.Invalid, _indexReader.GetStreamLastEventNumber("account--696193173"));
        }
    }

    [TestFixture]
    public class when_stream_is_in_of_range_of_read_limit : HashCollisionTestFixture{
        protected override void given(){
            _hashCollisionReadLimit = 5;
        }
        protected override void when(){
            //ptable 1
            _tableIndex.Add(1, "account--696193173", 0, 0);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 0, 3);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 1, 5);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 2, 7);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 3, 9);
            //mem table
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 4, 13);
        }
        [Test]
        public void should_return_last_event_number(){
            Assert.AreEqual(0, _indexReader.GetStreamLastEventNumber("account--696193173"));
        }
    }

    [TestFixture]
    public class when_hash_read_limit_is_not_reached : HashCollisionTestFixture{
        protected override void given(){
            _hashCollisionReadLimit = 3;
        }
        protected override void when(){
            //ptable 1
            _tableIndex.Add(1, "account--696193173", 0, 0);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 0, 3);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 1, 5);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 2, 7);
            _tableIndex.Add(1, "LPN-FC002_LPK51001", 3, 9);
        }
        [Test]
        public void should_return_last_event_number(){
            Assert.AreEqual(EventStore.Core.Data.EventNumber.Invalid, _indexReader.GetStreamLastEventNumber("account--696193173"));
        }
    }

    public class FakeIndexBackend : IIndexBackend {
        private TFReaderLease _readerLease;
        public FakeIndexBackend(TFReaderLease readerLease){
            _readerLease = readerLease;
        }
        public TFReaderLease BorrowReader(){
            return _readerLease;
        }

        public IndexBackend.EventNumberCached TryGetStreamLastEventNumber(string streamId){
            return new IndexBackend.EventNumberCached(-1, null); //always return uncached
        }
        public IndexBackend.MetadataCached TryGetStreamMetadata(string streamId) {
            return new IndexBackend.MetadataCached();
        }

        public int? UpdateStreamLastEventNumber(int cacheVersion, string streamId, int? lastEventNumber){
            return null;
        }
        public EventStore.Core.Data.StreamMetadata UpdateStreamMetadata(int cacheVersion, string streamId, EventStore.Core.Data.StreamMetadata metadata){
            return null;
        }

        public int? SetStreamLastEventNumber(string streamId, int lastEventNumber){
            return null;
        }
        public EventStore.Core.Data.StreamMetadata SetStreamMetadata(string streamId, EventStore.Core.Data.StreamMetadata metadata){
            return null;
        }

        public void SetSystemSettings(EventStore.Core.Data.SystemSettings systemSettings){}

        public EventStore.Core.Data.SystemSettings GetSystemSettings(){
            return null;
        }
    }

    public class FakeReader : ITransactionFileReader
    {
        public void Reposition(long position)
        {
            throw new NotImplementedException();
        }

        public SeqReadResult TryReadNext()
        {
            throw new NotImplementedException();
        }

        public SeqReadResult TryReadPrev()
        {
            throw new NotImplementedException();
        }

        public RecordReadResult TryReadAt(long position)
        {
            var record = (LogRecord)new PrepareLogRecord(position, Guid.NewGuid(), Guid.NewGuid(), 0, 0, position % 2 == 0 ? "account--696193173" : "LPN-FC002_LPK51001", -1, DateTime.UtcNow, PrepareFlags.None, "type", new byte[0], null);
            return new RecordReadResult(true, position + 1, record, 1);
        }

        public bool ExistsAt(long position)
        {
            return true;
        }
    }
}
