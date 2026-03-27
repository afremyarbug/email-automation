/**
 * Location picker: regions with nested cities, or a flat list when there is only one group (e.g. 50 cities).
 * Expects container with [data-pills], [data-search], [data-tree], [data-dropdown], [data-toggle-dropdown], [data-unselect].
 */
(function (global) {
  function LocationPicker(root, regionMap) {
    if (!root || !regionMap) return null;
    var selected = new Set();
    var expanded = new Set(Object.keys(regionMap));
    var searchQ = "";

    var pillsEl = root.querySelector("[data-pills]");
    var searchEl = root.querySelector("[data-search]");
    var treeEl = root.querySelector("[data-tree]");
    var dd = root.querySelector("[data-dropdown]");
    var btnOpen = root.querySelector("[data-toggle-dropdown]");
    var unsel = root.querySelector("[data-unselect]");

    function citiesInRegion(region) {
      return regionMap[region] || [];
    }

    function regionFullySelected(region) {
      var cities = citiesInRegion(region);
      if (cities.length === 0) return false;
      return cities.every(function (c) {
        return selected.has(c);
      });
    }

    function regionPartiallySelected(region) {
      var cities = citiesInRegion(region);
      var n = cities.filter(function (c) {
        return selected.has(c);
      }).length;
      return n > 0 && n < cities.length;
    }

    function toggleRegion(region, on) {
      citiesInRegion(region).forEach(function (c) {
        if (on) selected.add(c);
        else selected.delete(c);
      });
      render();
    }

    function toggleCity(city, on) {
      if (on) selected.add(city);
      else selected.delete(city);
      render();
    }

    function renderPills() {
      if (!pillsEl) return;
      pillsEl.innerHTML = "";
      var arr = Array.from(selected).sort();
      var maxShow = 8;
      arr.slice(0, maxShow).forEach(function (city) {
        var span = document.createElement("span");
        span.className = "loc-pill";
        span.appendChild(document.createTextNode(city + " "));
        var x = document.createElement("button");
        x.type = "button";
        x.className = "loc-pill-x";
        x.setAttribute("aria-label", "Remove");
        x.textContent = "×";
        x.addEventListener("click", function () {
          selected.delete(city);
          render();
        });
        span.appendChild(x);
        pillsEl.appendChild(span);
      });
      if (arr.length > maxShow) {
        var more = document.createElement("span");
        more.className = "loc-pill loc-pill-more";
        more.textContent = "+ " + (arr.length - maxShow) + " …";
        pillsEl.appendChild(more);
      }
    }

    function qLower() {
      return searchQ.trim().toLowerCase();
    }

    function regionMatches(region) {
      var q = qLower();
      if (!q) return true;
      if (region.toLowerCase().indexOf(q) !== -1) return true;
      return citiesInRegion(region).some(function (c) {
        return c.toLowerCase().indexOf(q) !== -1;
      });
    }

    function cityMatches(region, city) {
      var q = qLower();
      if (!q) return true;
      if (region.toLowerCase().indexOf(q) !== -1) return true;
      return city.toLowerCase().indexOf(q) !== -1;
    }

    function renderCityRows(region, container) {
      citiesInRegion(region).forEach(function (city) {
        if (!cityMatches(region, city)) return;
        var row = document.createElement("label");
        row.className = "loc-city-row";
        var ccb = document.createElement("input");
        ccb.type = "checkbox";
        ccb.checked = selected.has(city);
        ccb.addEventListener("change", function () {
          toggleCity(city, ccb.checked);
        });
        row.appendChild(ccb);
        row.appendChild(document.createTextNode(" " + city));
        container.appendChild(row);
      });
    }

    function renderTree() {
      if (!treeEl) return;
      treeEl.innerHTML = "";
      var regionKeys = Object.keys(regionMap).sort();

      // Single group (e.g. 50 cities): flat list — no expand/collapse step
      if (regionKeys.length === 1) {
        var region = regionKeys[0];
        if (!regionMatches(region)) return;

        var toolbar = document.createElement("div");
        toolbar.className = "loc-region-head loc-flat-toolbar";
        var cbAll = document.createElement("input");
        cbAll.type = "checkbox";
        cbAll.checked = regionFullySelected(region);
        cbAll.indeterminate = regionPartiallySelected(region);
        cbAll.addEventListener("change", function () {
          toggleRegion(region, cbAll.checked);
        });
        var labAll = document.createElement("span");
        labAll.className = "loc-region-label";
        labAll.textContent = "Select all";
        toolbar.appendChild(cbAll);
        toolbar.appendChild(labAll);
        treeEl.appendChild(toolbar);

        var childW = document.createElement("div");
        childW.className = "loc-city-list loc-city-list-flat";
        renderCityRows(region, childW);
        treeEl.appendChild(childW);
        return;
      }

      regionKeys.forEach(function (region) {
        if (!regionMatches(region)) return;

        var wrap = document.createElement("div");
        wrap.className = "loc-region";

        var head = document.createElement("div");
        head.className = "loc-region-head";

        var exp = document.createElement("button");
        exp.type = "button";
        exp.className = "loc-exp";
        exp.setAttribute("aria-label", "Expand or collapse cities");
        exp.textContent = expanded.has(region) ? "−" : "+";
        exp.addEventListener("click", function () {
          if (expanded.has(region)) expanded.delete(region);
          else expanded.add(region);
          renderTree();
        });

        var cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = regionFullySelected(region);
        cb.indeterminate = regionPartiallySelected(region);
        cb.addEventListener("change", function () {
          toggleRegion(region, cb.checked);
        });

        var lab = document.createElement("span");
        lab.className = "loc-region-label";
        lab.textContent = region;

        head.appendChild(exp);
        head.appendChild(cb);
        head.appendChild(lab);
        wrap.appendChild(head);

        if (expanded.has(region)) {
          var childW = document.createElement("div");
          childW.className = "loc-city-list";
          renderCityRows(region, childW);
          wrap.appendChild(childW);
        }

        treeEl.appendChild(wrap);
      });
    }

    function render() {
      renderPills();
      renderTree();
    }

    if (searchEl) {
      searchEl.addEventListener("input", function () {
        searchQ = searchEl.value || "";
        renderTree();
      });
    }

    if (unsel) {
      unsel.addEventListener("click", function (e) {
        e.preventDefault();
        selected.clear();
        render();
      });
    }

    var dropdownOpen = false;
    if (btnOpen && dd) {
      btnOpen.addEventListener("click", function () {
        dropdownOpen = !dropdownOpen;
        dd.hidden = !dropdownOpen;
        btnOpen.setAttribute("aria-expanded", dropdownOpen ? "true" : "false");
      });
    }

    render();

    return {
      getSelected: function () {
        return Array.from(selected);
      },
      selectCities: function (arr) {
        if (!arr || !arr.length) return;
        arr.forEach(function (city) {
          Object.keys(regionMap).forEach(function (region) {
            if (citiesInRegion(region).indexOf(city) !== -1) selected.add(city);
          });
        });
        render();
      },
      clear: function () {
        selected.clear();
        render();
      },
      setDisabled: function (d) {
        root.style.pointerEvents = d ? "none" : "";
        root.style.opacity = d ? "0.55" : "";
        root.querySelectorAll("input,button,select,textarea").forEach(function (el) {
          el.disabled = !!d;
        });
      },
    };
  }

  global.LocationPicker = LocationPicker;
})(typeof window !== "undefined" ? window : this);
