function getError(msg) {
    return "<div class=\"alert  alert-danger alert-dismissible fade show\" role=\"alert\">" +
        "     <span class=\"badge badge-pill badge-danger\"> Error </span> " + msg +
        "     <button type=\"button\" class=\"close\" data-dismiss=\"alert\" aria-label=\"Close\">" +
        "         <span aria-hidden=\"true\">×</span>" +
        "     </button>" +
        "</div>";
}

var site = "http://localhost:50000/";