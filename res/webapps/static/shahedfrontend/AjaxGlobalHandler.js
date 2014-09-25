
var AjaxGlobalHandler = {
    Initiate: function (options) {
        $.ajaxSetup({ cache: false });
        $(document).ajaxStart(function () {
            $.blockUI({
                message: options.AjaxWait.AjaxWaitMessage,
                css: options.AjaxWait.AjaxWaitMessageCss
            });
        }).ajaxSend(function (e, xhr, opts) {
        }).ajaxError(function (e, xhr, opts) {
            if (options.SessionOut.StatusCode == xhr.status) {
                document.location.replace(options.SessionOut.RedirectUrl);
                return;
            }
        }).ajaxSuccess(function (e, xhr, opts) {
        }).ajaxComplete(function (e, xhr, opts) {
        }).ajaxStop(function () {
            $.unblockUI();
        });
    }
};