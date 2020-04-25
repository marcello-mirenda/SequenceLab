using System;
using System.Collections.Generic;
using System.Text;

namespace MessageBrokerReceiver
{
    public class CountData : IJsonData
    {
        public int Count { get; set; }

        public string Message { get; set; }

        public CountData Clone()
        {
            return (CountData)MemberwiseClone();
        }

        public static bool operator ==(CountData a, CountData b)
        {
            return a.Equals(b);
        }

        public static bool operator !=(CountData a, CountData b)
        {
            return !a.Equals(b);
        }

        public override bool Equals(object obj)
        {
            return Count.Equals(((CountData)obj).Count) && Message.Equals(((CountData)obj).Message);
        }

        public override int GetHashCode()
        {
            return Count.GetHashCode();
        }

        public override string ToString()
        {
            return $"Count:{Count}, Message:{Message}";
        }
    }
}