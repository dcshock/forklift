$('.modifyBtns').click(function (evt) {
    evt.stopPropagation();
});
$("#filterButton").click(function() {
    var service = $(this).attr('service');
    swal({
        title: "Filter by Queue",
        text: "Input the queue you would like to view logs for",
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
        window.location = "/filtered?service="+service+"&queue="+inputValue;
        swal.close();
    });
});
$("#fixAllButton").click(function() {
    swal({
        title: "Fix All",
        text: "Set all logs as fixed for desired queue, no going back",
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
        $.post('/fixAll/', {
            queue: inputValue
        }, function() {
            location.reload();
        });

        swal.close();
    });
});
$('.retryButton').click(function () {
    var messageId = $(this).attr('messageId');
    var correlationId = $(this).attr('correlationId');
    var text = $(this).attr('text');
    var queue = $(this).attr('queue');
    $.post('/retry/', {
        correlationId: correlationId,
        text: text,
        queue: queue
    }, function() {
        $("#"+messageId).remove();
    });
});
$('.fixButton').click(function () {
    var messageId = $(this).attr('messageId');
    var updateId = $(this).attr('logId');
    var index = $(this).attr('index');
    $.post('/fixed/', {
        updateId: updateId,
        index: index
    }, function() {
        $("#"+messageId).remove();
    });
});
$('.changeQueueButton').click(function () {
    var messageId = $(this).attr('messageId');
    var correlationId = $(this).attr('correlationId');
    var text = $(this).attr('text');
    swal({
        title: "Change Queue",
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
        $.post('/retry/', {
            correlationId: correlationId,
            text: text,
            queue: inputValue
        }, function() {
            $("#"+messageId).remove();
        });
        swal.close();
    });
});
