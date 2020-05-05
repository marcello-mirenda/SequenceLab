namespace WorkerLogic
{
    public interface ILogger
    {
        void Information<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);

        void Information<T0, T1, T2>(string messageTemplate, T0 propertyValue0, T1 propertyValue1, T2 propertyValue2);

        void Warning<T0, T1, T2>(string messageTemplate, T0 propertyValue0, T1 propertyValue1, T2 propertyValue2);
    }
}