$env:SERILOG_WRITETOSEQSERVERURL="http://marcello-g3-3590:8015"
$env:RABBITMQ_HOSTNAME="marcello-g3-3590"
$env:MESSAGEBROKERSENDER_MAXTIMEOUT="00:00:05"

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

Start-MessageBrokerReceiver -topic topic1
Start-MessageBrokerReceiver -topic topic2
Start-MessageBrokerSender -topic topic1
Start-MessageBrokerSender -topic topic2

