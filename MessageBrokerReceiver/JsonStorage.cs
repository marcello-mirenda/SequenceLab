using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace MessageBrokerReceiver
{
    public static class JsonStorage
    {
        public static T Load<T>(string filePath) where T : IJsonData
        {
            using (var file = new StreamReader(filePath))
            {
                var serializer = new JsonSerializer();
                var reader = new JsonTextReader(file);
                return serializer.Deserialize<T>(reader);
            }
        }

        public static async Task<T> LoadAsync<T>(string filePath) where T : IJsonData
        {
            using (var file = new StreamReader(filePath))
            {
                var content = await file.ReadToEndAsync();
                file.Close();
                using (var sr = new StringReader(content))
                {
                    var serializer = new JsonSerializer();
                    var reader = new JsonTextReader(sr);
                    return serializer.Deserialize<T>(reader);
                }
            }
        }

        public static void Save<T>(this T data, string filePath) where T : IJsonData
        {
            using (StreamWriter file = File.CreateText(filePath))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(file, data);
                file.Close();
            }
        }

        public static async Task SaveAsync<T>(this T data, string filePath) where T : IJsonData
        {
            using (var sw = new StringWriter())
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(sw, data);
                using (var file = new StreamWriter(filePath))
                {
                    await file.WriteAsync(sw.GetStringBuilder());
                }
            }
        }
    }
}