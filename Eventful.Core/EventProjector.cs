using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Eventful
{
	public class EventProjector<TReadModel>
		where TReadModel : new()
	{
		private readonly EventfulFileSystemReader _reader;

		public EventProjector(EventfulFileSystemReader reader)
		{
			_reader = reader;
		}

		private readonly Dictionary<Type, Type> _knownProxies = new Dictionary<Type, Type>();

		public EventProjector<TReadModel> UseProxy<TType, TProxy>()
		{
			// TODO: Find a better way - proxies suck to create for ad hoc querying, and require all proxy properties to be hydrated, even if we only use some (conditionally)
			_knownProxies.Add(typeof(TProxy), typeof(TType));
			return this;
		}

		public TReadModel Run()
		{
			var readModel = new TReadModel();

			var handlers = typeof(TReadModel).GetMethods(BindingFlags.Public | BindingFlags.Instance)
				.Where(r => r.Name == "Handle")
				.ToDictionary(r => r.GetParameters().Single().ParameterType, r =>
				{
					var paramReadModel = Expression.Parameter(typeof(TReadModel), "r");
					var paramArg = Expression.Parameter(typeof(object), "o");

					var exprArg0 = Expression.Convert(paramArg, r.GetParameters().Single().ParameterType);
					var exprCall = Expression.Call(paramReadModel, r, exprArg0);

					var handlerDelegate = Expression.Lambda<Action<TReadModel, object>>(exprCall, new[] {paramReadModel, paramArg})
						.Compile();

					return (Action<object>)((o) => handlerDelegate(readModel, o));
				});
			
			_reader.Dispatch(_knownProxies, handlers);
			
			return readModel;
		}
	}
}