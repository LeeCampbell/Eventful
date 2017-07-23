using System;
using System.Collections.Generic;
using System.IO;

namespace Eventful
{
	public class EventfulFileSystemReader : IDisposable
	{
		private readonly FileStream _recordsStream;
		private readonly string _folder;

		private readonly Dictionary<string, ushort> _typeIdsMap = new Dictionary<string, ushort>();

		public EventfulFileSystemReader(string folder)
		{
			_folder = folder;

			_recordsStream = new FileStream(_folder + "\\records.idx", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 65536, FileOptions.SequentialScan);

			// Known Type Mapping
			using (var stream = new FileStream(_folder + "\\types.idx", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
			using (var reader = new BinaryReader(stream))
			{
				var length = reader.BaseStream.Length;
				while (reader.BaseStream.Position != length)
				{
					var typeId = reader.ReadUInt16();
					var typeName = reader.ReadString();

					_typeIdsMap.Add(typeName, typeId);
				}
			}
		}

		private readonly Dictionary<Type, Func<BinaryReader, object>> _customReaders =
			new Dictionary<Type, Func<BinaryReader, object>>();

		public void RegisterReader<TType>(Func<BinaryReader, TType> reader)
		{
			_customReaders.Add(typeof(TType), br => (object)reader(br));
		}

		public EventProjector<TReadModel> Project<TReadModel>()
			where TReadModel : new()
		{
			return new EventProjector<TReadModel>(this);
		}

		internal void Dispatch(Dictionary<Type, Type> knownProxies, Dictionary<Type, Action<object>> handlers)
		{
			// TODO: Maybe we should create a new reader each time?
			_recordsStream.Seek(0, SeekOrigin.Begin);

			var readerStream = new UInt16Reader(_recordsStream);

			// TODO: Maybe we should share column readers?
			var typesMap = new Type[ushort.MaxValue];
			var typeReaders = new TypeReader[ushort.MaxValue];

			foreach (var type in handlers.Keys)
			{
				var originalType = type;

				if (knownProxies.ContainsKey(type))
					originalType = knownProxies[type];

				if (_typeIdsMap.ContainsKey(originalType.FullName))
				{
					var typeId = _typeIdsMap[originalType.FullName];
					var path = _folder + "\\" + originalType.FullName + "\\";

					typesMap[typeId] = type;
					typeReaders[typeId] = new TypeReader(path, type, _customReaders)
					{
						Handler = handlers[type]
					};
				}
			}
			
			// Keep reading while there's more data
			ushort[] records = null;
			while ((records = readerStream.ReadUInt16s()).Length != 0)
			{
				// Read as many values as we can from the reader's buffer
				for (var i = 0; i < records.Length; i++)
				{
					var typeId = records[i];
					var typeReader = typeReaders[typeId];

					if (typeReader == null)
						continue;

					var handler = typeReader.Handler;
					var instance = typeReader.Read();

					handler(instance);
				}
			}
			
			foreach (var type in typeReaders)
			{
				if (type == null)
					continue;

				type.Dispose();
			}
		}

		public void Dispose()
		{
			_recordsStream.Dispose();
		}
	}
}