param ([switch]$StartMessageBrokerWorker)

$env:SERILOG_WRITETOSEQSERVERURL="http://marcello-g3-3590:8015"
$env:RABBITMQ_HOSTNAME="marcello-g3-3590"
$env:MESSAGEBROKERSENDER_MAXTIMEOUT="00:00:01"
$env:KAFKA_BOOTSTRAPSERVERS="marcello-g3-3590:9092"
$env:KAFKA_COMMITPERIOD=5

function Start-MessageBrokerWorker
{
    param ($topic)
    $WorkingFolder = ".\MessageBrokerWorker\bin\Debug\netcoreapp3.1\"
    Start-Process -FilePath "${WorkingFolder}MessageBrokerWorker.exe" `
        -WorkingDirectory $WorkingFolder `
        -ArgumentList "--topic $topic --group-id mmi-worker --file E:\Work\SequenceLab.json" `
        -NoNewWindow
}

function Start-MessageBrokerReceiver
{
    param ($topic)
    $WorkingFolder = ".\MessageBrokerReceiver\bin\Debug\netcoreapp3.1\"
    Start-Process -FilePath "${WorkingFolder}MessageBrokerReceiver.exe" `
        -WorkingDirectory $WorkingFolder `
        -ArgumentList "--topic $topic --file E:\Work\SequenceLab.json" `
        -NoNewWindow
}

function Start-MessageBrokerSender
{
    param ($topic)
    $WorkingFolder = ".\MessageBrokerSender\bin\Debug\netcoreapp3.1\"
    Start-Process -FilePath "${WorkingFolder}MessageBrokerSender.exe" `
        -ArgumentList "--topic $topic" `
        -WorkingDirectory $WorkingFolder
}

if ($StartMessageBrokerWorker.IsPresent)
{
    Start-MessageBrokerWorker -topic topic1
}
else
{
    Start-MessageBrokerWorker -topic topic1
    Start-MessageBrokerWorker -topic topic1

    Start-Sleep -Seconds 2

    Start-MessageBrokerReceiver -topic topic1
    Start-MessageBrokerReceiver -topic topic1
    Start-MessageBrokerReceiver -topic topic1
    Start-MessageBrokerReceiver -topic topic1
    Start-MessageBrokerReceiver -topic topic1

    Start-Sleep -Seconds 2

    Start-MessageBrokerSender -topic topic1
}
