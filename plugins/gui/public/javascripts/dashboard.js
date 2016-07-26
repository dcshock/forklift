var t = new Object();

var logHistory = []
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
        $(".navbar-brand").html("Forklift GUI");
        reset();
        clearConsole();
    });


    $("#console-list-group").on('click', '.retryButton', function(e) {
        selectedId = $(this).attr("id");
        $(this).parent().remove();
        retry(findLog(selectedId));
        e.preventDefault();
    });

    $("#console-list-group").on('click', '.fixedButton', function(e) {
        selectedId = $(this).attr("id");
        $(this).parent().remove();
        fixed(findLog(selectedId));
        e.preventDefault();
    });

    $("#console-list-group").on('click', '.fixAllButton', function(e) {
        selectedId = $(this).attr("id");
        var matches = findAllIdsWithMatchingQueue(selectedId);
        swal({
                title: "Are you sure?",
                text: "You will not be able to undo this action!!",
                type: "warning",
                showCancelButton: true,
                confirmButtonClass: "btn-danger",
                confirmButtonText: "Yes, fix all!",
                cancelButtonText: "Cancel",
                closeOnConfirm: false,
                closeOnCancel: true
            },
            function(isConfirm) {
                if (isConfirm) {
                    fixMultiple(matches);
                    reset();
                    clearConsole();
                    e.preventDefault();
                    swal("Done!", "All documents of matching queue have been marked as fixed!", "success");
                }
            });

    });

    $("#console-list-group").on('click', '.changeQueueButton', function(e) {
        e.preventDefault();
        selectedId = $(this).attr("id");
        $(this).parent().remove();
        swal({
            title: "Desired Queue?",
            text: "please input the queue you'd like this document to be sent",
            type: "input",
            showCancelButton: true,
            closeOnConfirm: false,
            inputPlaceholder: "desired queue..."
        }, function(inputValue) {
            if (inputValue === false) return false;
            if (inputValue === "") {
                swal.showInputError("You need to provide a queue");
                return false;
            }
            retryWithQueue(findLog(selectedId), inputValue);
            swal.close();
        });
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

function findLog(id) {
    for(var i = 0; i < logHistory.length; i++) {
        if (logHistory[i].id == id) {
            log = logHistory[i];
            return log;
        }
    }
    return null;
}

function findAllIdsWithMatchingQueue(id) {
    var log = findLog(id);
    var queue = log.queue;
    var matches = [];
    for(var i = 0; i < logHistory.length; i++) {
        if (logHistory[i].queue == queue) {
            var match = {
                id: logHistory[i].id,
                index: logHistory[i].index
            }
            matches.push(match);
        }
    }
    return matches;
}

function reset() {
    $.fn.overlayout();
    logStack = [];
    logHistory = [];
    idStack = [];
    selectedId = null;
    newestId = null;
    $("#console-list-group").html("");
    $("#errorPlaceholder").addClass("hidden");
    clearTimeout(displayTimer);
    clearTimeout(overlayTimer);
    displayLogs();
}

function clearConsole() {
    $("#welcome").addClass("hidden");
    $("#no-logs").addClass("hidden");
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
            $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching forklift logs.<br><p class='red'>" + err.message + "</p></h3>")
        } else if (err.status != "200") {
            if (err.status == "404") {
                var date = new Date();
                $("#errorPlaceholder").html("<h3 class='error'>No logs were found under index: forklift-"+currentService+"*<br><p class='red'>Status 404 returned.</p></h3>")
            } else {
                $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching forklift logs.<br><p class='red'>Status " + err.status + ": " + err.message + "</p></h3>")
            }
        } else {
            $("#errorPlaceholder").html("<h3 class='error'>" + err.message + "</h3>")
        }
    } else {
        $("#errorPlaceholder").html("<h3 class='error'>There was a problem fetching forklift logs.<br><p class='red'>" + err.message + "</h3>")
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

function pollElasticSearch() {
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

function retry(log) {
    if (log != null) {
        $.post(
            "dashboard/retry/",
            {
                correlationId: log.correlationId,
                text: log.text,
                queue: log.queue
            }
        )
    }
}

function retryWithQueue(log, queue) {
    if (log != null) {
        $.post(
            "dashboard/retry/", {
                correlationId: log.correlationId,
                text: log.text,
                queue: queue
            }
        )
    }
}

function fixed(log) {
    if (log != null) {
        $.post(
            "dashboard/fixed/",
            {
                id: log.id,
                index: log.index
            }
        )
    }
}

function fixMultiple(list) {
    list.forEach(function(entry) {
        $.post("dashboard/fixed/",{
            id: entry.id,
            index: entry.index
        });
    });
}

function addLogToStack(logs) {
    //we got a valid response, remove service errors if they exist
    if (logs.length > 0)
    {
        $("#no-logs").addClass("hidden");
        logs.forEach(function (log) {
            var logSource = log._source;
            var retryCount = null;
            var maxRetries = null;
            if (currentService == "replay") {
                retryCount = logSource["forklift-retry-count"];
                maxRetries = logSource["forklift-retry-max-retries"];
            }
            if (currentService == "retry" || (currentService == "replay" && (retryCount == maxRetries))) {
                var messageId = null;
                var errors = logSource.errors;
                var text = logSource.text;
                var queue = logSource.queue;
                var correlationId = null;

                var messageHtml = null;
                var buttonHtml = '';
                var retryHtml = '';
                var correlationHtml = '';
                if (currentService == "retry") {
                    messageId = JSON.parse(logSource["forklift-retry-msg"]).messageId;
                    correlationId = JSON.parse(logSource["forklift-retry-msg"]).correlationId;
                    if (!correlationId) {
                        correlationId = messageId;
                    }
                    messageHtml = '<strong>Message ID:</strong> ' + messageId;
                    correlationHtml = '<br><strong>Correlation ID: </strong>' + correlationId;
                    retryHtml = '<br><strong>Retry Count: </strong> ' + logSource["forklift-retry-count"] + ' / ' + logSource["forklift-retry-max-retries"];
                } else {
                    messageId = log._id;
                    messageHtml = '<br><strong>Message ID:</strong> ' + messageId;
                    correlationHtml = '<br><strong>Correlation ID: </strong>' + messageId;
                    correlationId = messageId;
                    buttonHtml = '<button id="' + log._id + '"class="btn btn-warning retryButton"><span class="glyphicon glyphicon-repeat"></span> Retry</button> ' +
                                '<button id="' + log._id + '"class="btn btn-warning changeQueueButton"><span class="glyphicon glyphicon-repeat"></span> Change Queue</button> ' +
                                '<button id="' + log._id + '" class="btn btn-success fixedButton"><span class="glyphicon glyphicon-ok"></span> Fixed</button> ' +
                                '<button id="' + log._id + '" class="btn btn-danger fixAllButton"><span class="glyphicon glyphicon-fire"></span> Fix All (of matching queue)</button>';
                }

                selectedId = log._id;
                var timestamp = logSource.time;
                var docDate = timestamp.split("T")[0].replace(/-/g, "");

                var logHtml = {
                    index: log._index,
                    id: log._id,
                    date: docDate,
                    service: currentService,
                    queue: queue,
                    text: text,
                    correlationId: correlationId,
                    html: '<a href="#" class="list-group-item list-group-item-default">' +
                    buttonHtml +
                    '<span class="badge pull-right alert-default">' + timestamp + '</span>' +
                    messageHtml +
                    '<br>' +
                    '<strong>Queue:</strong> ' + queue +
                    '<br>' +
                    '<strong>Errors:</strong> ' + errors +
                    '<br>' +
                    '<strong>Text:</strong> ' + text +
                    correlationHtml +
                    retryHtml +
                    '</a>'
                }
                logStack.push(logHtml);
                if (currentService == "retry") {
                }
                logHistory.push(logHtml);
                displayLogs();
            } else {
                $.fn.overlayout();
            }
            clearTimeout(overlayTimer);
        });
    } else {
        $.fn.overlayout();
        $("#no-logs").removeClass("hidden");
    }
}
t.preload();
