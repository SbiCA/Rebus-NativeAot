using System.Dynamic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using System.Text.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Rebus.NativeAot;
using Sparrow.Json.Parsing;
using JsonSerializer = System.Text.Json.JsonSerializer;


namespace Rebus.NativeAot;

// TODO: this needs to implement probably same feature set as https://github.com/ravendb/ravendb/blob/f9e1a5717e164f37a287107a9ce2831e50575049/src/Raven.Client/Json/Serialization/NewtonsoftJson/NewtonsoftJsonSerializationConventions.cs
public class SystemTextJsonSerializationConventions : ISerializationConventions
{
    private IBlittableJsonConverter _defaultConverter;
    private JsonSerializerOptions _serializerOptions;
    private Action<JsonSerializerOptions> _customizeJsonSerializer;
    private Action<JsonSerializerOptions> _customizeJsonDeserializer;
    private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;

    public DocumentConventions Conventions { get; private set; }

    public SystemTextJsonSerializationConventions()
    {
        _defaultConverter = new BlittableJsonConverter(this);
        _serializerOptions = AppJsonSerializerContext.Default.Options;
        CustomizeJsonSerializer = _ => { };
        CustomizeJsonDeserializer = _ => { };
    }

    public Action<JsonSerializerOptions> CustomizeJsonSerializer
    {
        get => _customizeJsonSerializer;
        set
        {
            // Conventions?.AssertNotFrozen();
            _customizeJsonSerializer = value;
        }
    }

    public Action<JsonSerializerOptions> CustomizeJsonDeserializer
    {
        get => _customizeJsonDeserializer;
        set
        {
            // Conventions?.AssertNotFrozen();
            _customizeJsonDeserializer = value;
        }
    }

    public Func<Type, BlittableJsonReaderObject, object> DeserializeEntityFromBlittable
    {
        get => _deserializeEntityFromBlittable;
        set
        {
            // Conventions?.AssertNotFrozen();
            _deserializeEntityFromBlittable = value;
        }
    }

    public void Initialize(DocumentConventions conventions)
    {
        Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        if (_deserializeEntityFromBlittable == null)
        {
            _deserializeEntityFromBlittable = (type, json) => JsonSerializer.Deserialize(json.ToString(), type, _serializerOptions);
        }
    }

    public IBlittableJsonConverter DefaultConverter => _defaultConverter;

    public ISessionBlittableJsonConverter CreateConverter(InMemoryDocumentSessionOperations session)
    {
        return new SessionBlittableJsonConverter(session);
    }

    public IJsonSerializer CreateDeserializer(CreateDeserializerOptions options = null)
    {
        var optionsCopy = new JsonSerializerOptions(_serializerOptions);
        CustomizeJsonSerializer(optionsCopy);
        CustomizeJsonDeserializer(optionsCopy);
        return new SystemTextJsonJsonSerializer(optionsCopy);
    }

    public IJsonSerializer CreateSerializer(CreateSerializerOptions options = null)
    {
        var optionsCopy = new JsonSerializerOptions(_serializerOptions);
        CustomizeJsonSerializer(optionsCopy);
        return new SystemTextJsonJsonSerializer(optionsCopy);
    }

    public IJsonWriter CreateWriter(JsonOperationContext context)
    {
        return new BlittableJsonWriter(context);
    }

    public object? DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json)
    {
        return JsonSerializer.Deserialize(json.ToString(), type, _serializerOptions);
    }

    public T? DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json)
    {
        return JsonSerializer.Deserialize<T>(json.ToString(), _serializerOptions);
    }
}


// needs to be copied because it's internal
internal sealed class SessionBlittableJsonConverter : BlittableJsonConverterBase, ISessionBlittableJsonConverter
}
    
    // needs to be copied because it's internal
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        protected readonly ISerializationConventions Conventions;

        protected BlittableJsonConverterBase(ISerializationConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json)
        {
            var jsonSerializer = Conventions.CreateSerializer();
            PopulateEntity(entity, json, jsonSerializer);
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (json == null)
                throw new ArgumentNullException(nameof(json));
            if (jsonSerializer == null)
                throw new ArgumentNullException(nameof(jsonSerializer));

            var serializer = (JsonSerializer)jsonSerializer;
            var old = serializer.ObjectCreationHandling;
            serializer.ObjectCreationHandling = ObjectCreationHandling.Replace;

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Initialize(json);

                    serializer.Populate(reader, entity);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity.", ex);
            }
            finally
            {
                serializer.ObjectCreationHandling = old;
            }
        }

        protected static BlittableJsonReaderObject ToBlittableInternal(
             object entity,
             DocumentConventions conventions,
             JsonOperationContext context,
             IJsonSerializer serializer,
             IJsonWriter writer,
             bool removeIdentityProperty = true)
        {
            var usesDefaultContractResolver = ((JsonSerializer)serializer).ContractResolver.GetType() == typeof(DefaultRavenContractResolver);
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;
            var willUseDefaultContractResolver = usesDefaultContractResolver && isDynamicObject == false;
            var hasIdentityProperty = conventions.GetIdentityProperty(type) != null;

            if (willUseDefaultContractResolver)
            {
                DefaultRavenContractResolver.RootEntity = removeIdentityProperty && hasIdentityProperty ? entity : null;
                DefaultRavenContractResolver.RemovedIdentityProperty = false;

                // PERF: By moving the try..finally statement we forgo the need for prolog and epilog when it is not needed.
                try
                {
                    serializer.Serialize(writer, entity);
                }
                finally
                {
                    DefaultRavenContractResolver.RootEntity = null;
                }
            }
            else
            {
                serializer.Serialize(writer, entity);
            }

            writer.FinalizeDocument();

            var reader = writer.CreateReader();

            if (willUseDefaultContractResolver == false || hasIdentityProperty && DefaultRavenContractResolver.RemovedIdentityProperty == false)
            {
                //This is to handle the case when user defined it's own contract resolver
                //or we are serializing dynamic object

                var changes = removeIdentityProperty && TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
                changes |= TrySimplifyJson(reader, type);

                if (changes)
                {
                    using (var old = reader)
                    {
                        reader = context.ReadObject(reader, "convert/entityToBlittable");
                    }
                }
            }

            return reader;
        }

        private static bool TryRemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConventions conventions, bool isDynamicObject)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
            {
                if (conventions.AddIdFieldToDynamicObjects && isDynamicObject)
                {
                    if (document.Modifications == null)
                        document.Modifications = new DynamicJsonValue(document);

                    document.Modifications.Remove("Id");
                    return true;
                }

                return false;
            }

            if (document.Modifications == null)
                document.Modifications = new DynamicJsonValue(document);

            document.Modifications.Remove(conventions.GetConvertedPropertyNameFor(identityProperty));
            return true;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderObject document, Type rootType)
        {
            var simplified = false;
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyType = GetPropertyType(propertyName, rootType);
                if (propertyType == typeof(JObject) || propertyType == typeof(JArray) || propertyType == typeof(JValue))
                {
                    // don't simplify the property if it's a JObject
                    continue;
                }

                var propertyValue = document[propertyName];

                if (propertyValue is BlittableJsonReaderArray propertyArray)
                {
                    simplified |= TrySimplifyJson(propertyArray, propertyType);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                if (propertyObject.TryGet(Constants.Json.Fields.Type, out string type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject, propertyType);
                    continue;
                }

                if (ShouldSimplifyJsonBasedOnType(type) == false)
                    continue;

                simplified = true;

                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                if (propertyObject.TryGet(Constants.Json.Fields.Values, out BlittableJsonReaderArray values) == false)
                {
                    if (propertyObject.Modifications == null)
                        propertyObject.Modifications = new DynamicJsonValue(propertyObject);

                    propertyObject.Modifications.Remove(Constants.Json.Fields.Type);
                    continue;
                }

                document.Modifications[propertyName] = values;

                simplified |= TrySimplifyJson(values, propertyType);
            }

            return simplified;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderArray array, Type rootType)
        {
            var itemType = GetItemType();

            var simplified = false;
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                simplified |= TrySimplifyJson(itemObject, itemType);
            }

            return simplified;

            Type GetItemType()
            {
                if (rootType == null)
                    return null;

                if (rootType.IsArray)
                    return rootType.GetElementType();

                var enumerableInterface = rootType.GetInterface(typeof(IEnumerable<>).Name);
                if (enumerableInterface == null)
                    return null;

                return enumerableInterface.GenericTypeArguments[0];
            }
        }

        private static bool ShouldSimplifyJsonBasedOnType(string typeValue)
        {
            var type = Type.GetType(typeValue);

            if (type == null)
                return false;

            if (type.IsArray)
                return true;

            if (type.GetGenericArguments().Length == 0)
                return type == typeof(Enumerable);

            return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
        }

        internal static Type GetPropertyType(string propertyName, Type rootType)
        {
            if (rootType == null)
                return null;

            MemberInfo memberInfo = null;
            try
            {
                memberInfo = ReflectionUtil.GetPropertyOrFieldFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic, propertyName);
            }
            catch (AmbiguousMatchException)
            {
                var memberInfos = ReflectionUtil.GetPropertiesAndFieldsFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic)
                    .Where(x => x.Name == propertyName)
                    .ToList();

                while (typeof(object) != rootType)
                {
                    memberInfo = memberInfos.FirstOrDefault(x => x.DeclaringType == rootType);
                    if (memberInfo != null)
                        break;

                    if (rootType.BaseType == null)
                        break;

                    rootType = rootType.BaseType;
                }
            }

            switch (memberInfo)
            {
                case PropertyInfo pi:
                    return pi.PropertyType;
                case FieldInfo fi:
                    return fi.FieldType;
                default:
                    return null;
            }
        }
    }
    
public static class ServiceCollectionExtensions
{
    public static IDocumentStore AddRavenDb(this IServiceCollection services, IConfiguration configuration, DocumentConventions? conventions = null)
    {
        conventions.Serialization = new SystemTextJsonSerializationConventions();
        var (dbSettings, cert) = DatabaseSetting.FromConfig(configuration);
        var store = new DocumentStore
        {
            Urls = dbSettings.Urls,
            Database = dbSettings.DatabaseName,
            Certificate = cert,
            Conventions = conventions
        }.Initialize();

        services.AddSingleton(store);
        return store;
    }

    public static void ApplyDocumentConventions(this IDocumentStore documentStore, DocumentConventions documentConventions)
    {
        documentStore.Conventions.FindIdentityProperty = documentConventions.FindIdentityProperty;
        documentStore.Conventions.FindCollectionName = documentConventions.FindCollectionName;
        documentStore.Conventions.Serialization = documentConventions.Serialization;
    }
}

public class DatabaseSetting
{
    public string[] Urls { get; set; }
    public string DatabaseName { get; set; }
    public string CertPath { get; set; }
    public string CertPass { get; set; }

    public X509Certificate2 Certificate2 => !string.IsNullOrEmpty(CertPath)
        ? new X509Certificate2(CertPath, CertPass)
        : null;

    public static (DatabaseSetting, X509Certificate2) FromConfig(IConfiguration configuration, string sectionName = null)
    {
        var dbSettings = new DatabaseSetting();
        configuration.Bind(sectionName ?? nameof(DatabaseSetting), dbSettings);
        var certificate = dbSettings.Certificate2;
        return (dbSettings, certificate);
    }
}