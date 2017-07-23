using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Eventful
{
	internal class TypeReader : IDisposable
	{
		public Action<object> Handler { get; set; }

		private readonly ColumnReader[] _columns;
		private readonly Func<object> _instanceFactory;
		private readonly object _instanceReusable;

		public TypeReader(string folder, Type type, Dictionary<Type, Func<BinaryReader, object>> customReaders)
		{
			_instanceReusable = Activator.CreateInstance(type);

			var properties = type
				.GetProperties(BindingFlags.Instance | BindingFlags.Public)
				.Where(p => p.CanRead && p.CanWrite);

			_columns = properties.Select(p => new ColumnReader(folder, p, customReaders)).ToArray();

			_instanceFactory = Expression.Lambda<Func<object>>(Expression.New(type)).Compile();
		}

		public object Read()
		{
			var instance = _instanceReusable;

			for (var j = 0; j < _columns.Length; j++)
			{
				_columns[j].Read(instance);
			}

			return instance;
		}

		public object ReadNew()
		{
			var instance = _instanceFactory();

			for (var j = 0; j < _columns.Length; j++)
			{
				_columns[j].Read(instance);
			}

			return instance;
		}

		public void Dispose()
		{
			foreach (var column in _columns)
			{
				column.Dispose();
			}
		}
	}
}