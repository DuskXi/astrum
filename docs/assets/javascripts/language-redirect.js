(function () {
  var storageKey = "astrum.docs.languageRedirect.checked";
  var scriptSuffix = "/assets/javascripts/language-redirect.js";

  function getBasePath() {
    var script = document.currentScript;
    if (!script || !script.src) {
      return "/";
    }

    try {
      var scriptUrl = new URL(script.src);
      var pathname = scriptUrl.pathname;
      if (pathname.endsWith(scriptSuffix)) {
        var base = pathname.slice(0, -scriptSuffix.length);
        return base.endsWith("/") ? base : base + "/";
      }
    } catch (_error) {
      return "/";
    }

    return "/";
  }

  function isHomePage(pathname, basePath) {
    return pathname === basePath || pathname === basePath + "index.html";
  }

  function preferredSupportedLanguage() {
    var languages = navigator.languages && navigator.languages.length
      ? navigator.languages
      : [navigator.language || navigator.userLanguage || ""];

    for (var index = 0; index < languages.length; index += 1) {
      var language = String(languages[index]).toLowerCase();
      if (language.startsWith("zh")) {
        return "zh";
      }
      if (language.startsWith("en")) {
        return "en";
      }
    }

    return "zh";
  }

  try {
    if (window.localStorage.getItem(storageKey)) {
      return;
    }

    var basePath = getBasePath();
    var currentPath = window.location.pathname;

    if (!isHomePage(currentPath, basePath)) {
      return;
    }

    window.localStorage.setItem(storageKey, "1");

    if (preferredSupportedLanguage() === "en") {
      window.location.replace(basePath + "en/" + window.location.search + window.location.hash);
    }
  } catch (_error) {
    return;
  }
})();
