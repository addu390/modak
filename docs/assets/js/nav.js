/* Expand the first two sidebar sections by default. */
document.addEventListener("DOMContentLoaded", function () {
  var items = document.querySelectorAll(
    ".md-nav--primary > .md-nav__list > .md-nav__item--nested"
  );
  Array.prototype.slice.call(items, 0, 2).forEach(function (item) {
    var toggle = item.querySelector(":scope > .md-nav__toggle");
    if (toggle) toggle.checked = true;
  });
});
