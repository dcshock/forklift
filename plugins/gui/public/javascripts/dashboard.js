$('[data-toggle="tooltip"]').tooltip();
$(".fixBtn").click(function() {
    var queue = $(this).attr('queue');
    swal({
        title: "Are you sure?",
        text: "This will set all [ " + queue + " ] replay logs as fixed.",
        type: "warning",
        showCancelButton: true,
        confirmButtonText: 'Yes, Fix them!'
    }, function() {
        $.post('fixAll', {
            queue: queue
        }, function() {
            setTimeout(function() {
                location.reload();
            }, 2000);
        });
    });
});
$(".retryBtn").click(function() {
    var queue = $(this).attr('queue');
    swal({
        title: "Are you sure?",
        text: "This will retry all [ " + queue + " ] replay logs.",
        type: "warning",
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: 'Yes, Retry them!'
    }, function() {
        $.post('retryAll', {
            queue: queue
        }, function() {
            setTimeout(function() {
                location.reload();
            }, 2000);
        });
    });
});
