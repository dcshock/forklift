var t = new Object();

var logStack = [];
var idStack = [];
var selectedId = null;
var newestId = null;
var currentService = null;

var displayTimer = null;
var overlayTimer = null;

t.preload = function () {
    hljs.initHighlightingOnLoad();
    $('[data-toggle="tooltip"]').tooltip();

    $("#home").click(function() {
        $(".navbar-brand").html("Noise");
        reset();
        clearConsole();
    });


    $("#console-list-group").on('click', '.retryButton', function(e) {
        selectedId = $(this).attr("id");
        $(this).parent().remove();
        console.log("retrying "+selectedId);
        e.preventDefault();
    });

    $("#console-list-group").on('click', '.fixedButton', function(e) {
        selectedId = $(this).attr("id");
        console.log("fixed "+selectedId);
        $(this).parent().remove();
        e.preventDefault();
    });


    $("#dashboardMenu li a").on('click', function() {
        reset();
        if ($("#liveLogs").hasClass("hidden")) {
            $("#console").overlay();
        } else {
            $("#liveLogs").overlay();
        }
        $("#welcome").addClass("hidden");
        $(".navbar-brand").html($(this).text());
        currentService = $(this).attr('name');
        pollElasticSearch();

    });
}

function reset() {
    $.fn.overlayout();
    logStack = [];
    idStack = [];
    selectedId = null;
    newestId = null;
    $("#console-list-group").html("");
    $("#errorPlaceholder").addClass("hidden");
    clearTimeout(displayTimer);
    clearTimeout(overlayTimer);
    displayLogs();
}

function timeoutReset() {
    logStack = [];
    idStack = [];
    selectedId = null;
    newestId = null;
    clearTimeout(displayTimer);
    clearTimeout(overlayTimer);
}

function clearConsole() {
    $("#welcome").addClass("hidden");
    $("#liveLogs").addClass("hidden");
    $("#errorPlaceholder").addClass("hidden");
    $("#welcome").removeClass("hidden");
    clearTimeout(displayTimer);
    clearTimeout(overlayTimer);
}
function displayLogs() {
    if (logStack.length > 0) {
        $.fn.overlayout();
        $("#liveLogs").removeClass("hidden");
        $("#errorPlaceholder").addClass("hidden");
    }
    displayTimer = setTimeout(function() {
            if (logStack.length > 0) {
                var log = logStack.pop();
                $("#console-list-group").prepend(log.html);
            }
        displayLogs();
    }, 100)
}

function displayErrorWarning(err) {
    $.fn.overlayout();
    $("#console-list-group").html("");
    $("#errorPlaceholder").removeClass("hidden");
    $("#liveLogs").addClass("hidden");
    if (err.status) {
        if (err.status == "timeout") {
            $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching logs.<br><p class='red'>" + err.message + "</p></h3>")
        } else if (err.status != "200") {
            if (err.status == "404") {
                var date = new Date();
                $("#errorPlaceholder").html("<h3 class='error'>No logs were found under index: logstash-" + date.getFullYear() + "." + date.getMonth() + 1 + "." + date.getDate() + "<br><p class='red'>Status 404 returned.</p></h3>")
            } else {
                $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching logs.<br><p class='red'>Status " + err.status + ": " + err.message + "</p></h3>")
            }
        } else {
            $("#errorPlaceholder").html("<h3 class='error'>" + err.message + "</h3>")
        }
    } else {
        $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching logs.<br><p class='red'>" + err.message + "</h3>")
    }
    clearTimeout(overlayTimer);
}

function displayCustomError(title, message) {
    $.fn.overlayout();
    $("#console-list-group").html("");
    $("#errorPlaceholder").removeClass("hidden");
    $("#liveLogs").addClass("hidden");
    $("#errorPlaceholder").html("<h3 class='error'>"+title+"<br><p class='red'>" + message + "</h3>")
    clearTimeout(overlayTimer);
}
function pollElasticSearch () {
    $.post(
        "dashboard/poll/",
        {service: currentService},
        function (resp) {
            var jsonObj = JSON.parse(resp);
            if (jsonObj.log == true) {
                addLogToStack(jsonObj.response);
            } else {
                displayErrorWarning(jsonObj.response);
            }
        }
    );
}

function addLogToStack(logs) {
    //we got a valid response, remove service errors if they exist
    if (logs.length > 0)
    {
        logs.forEach(function (log) {
            var logSource = log._source;
            var messageId = null
            var retryCount = 0;//logSource.forklift-retry-count;
            var maxRetries = 0;//logSource.forklift-retry-max-retries;
            var errors = logSource.errors;
            var text = logSource.text;
            var queue = logSource.queue;

            var messageHtml = null;
            if (currentService == "retry") {
                messageId = JSON.parse(logSource["forklift-retry-msg"]).messageId;
                messageHtml = '<strong>Message ID:</strong> ' + messageId;
            } else {
                messageId = log._id;
                messageHtml = '<strong>Message ID:</strong> ' + messageId;
            }

            selectedId = log._id;
            var timestamp = logSource.time;;

            var logHtml = {
                id: log._id,
                html: '<a href="#" class="list-group-item list-group-item-default">' +
                '<button id="' + log._id + '"class="btn btn-default retryButton"><span class="glyphicon glyphicon-repeat"></span> Retry</button>' +
                ' <button id="' + log._id + '" class="btn btn-default fixedButton"><span class="glyphicon glyphicon-ok"></span> Fixed</button>' +
                '<span class="badge pull-right alert-default">' + timestamp + '</span>' +
                '<br>' +
                messageHtml +
                '<br>' +
                '<strong>Queue:</strong> ' + queue +
                '<br>' +
                '<strong>Errors:</strong> ' + errors +
                '<br>' +
                '<strong>Text:</strong> '+text+'</a>'
            }
            logStack.push(logHtml);
            displayLogs();
            clearTimeout(overlayTimer);
        });
    } else {
        displayCustomError("No logs were found!", "Are logs being pushed to ElasticSearch?");
    }
}
t.preload();
