/* MK7 Media — Country-code phone input
 * Wraps any [data-phone-input] container that has an <input type="tel">.
 * Inserts a country-code <select> in front of the input, defaults to Lebanon,
 * remembers the user's pick in localStorage, and exposes helpers used by
 * form-submit code to read the full E.164 number.
 *
 * Usage:
 *   <div data-phone-input>
 *     <input type="tel" id="q_whatsapp" placeholder="WhatsApp number" required>
 *   </div>
 *
 *   const fullNum = window.PhoneInput.fullNumber(document.getElementById('q_whatsapp'));
 *   const valid   = window.PhoneInput.isValid(document.getElementById('q_whatsapp'));
 */
(function () {
  'use strict';

  // Most-used markets first (Lebanon default), GCC/MENA priority block,
  // then North America + Europe. Iso2 used as the option value so we can
  // distinguish e.g. US vs Canada (both +1) when remembering the user's pick.
  var COUNTRIES = [
    { iso: 'LB', code: '+961', flag: '🇱🇧', name: 'Lebanon',     priority: true },
    { iso: 'AE', code: '+971', flag: '🇦🇪', name: 'UAE',         priority: true },
    { iso: 'SA', code: '+966', flag: '🇸🇦', name: 'Saudi Arabia',priority: true },
    { iso: 'KW', code: '+965', flag: '🇰🇼', name: 'Kuwait',      priority: true },
    { iso: 'QA', code: '+974', flag: '🇶🇦', name: 'Qatar',       priority: true },
    { iso: 'BH', code: '+973', flag: '🇧🇭', name: 'Bahrain',     priority: true },
    { iso: 'OM', code: '+968', flag: '🇴🇲', name: 'Oman',        priority: true },
    { iso: 'JO', code: '+962', flag: '🇯🇴', name: 'Jordan',      priority: true },
    { iso: 'EG', code: '+20',  flag: '🇪🇬', name: 'Egypt',       priority: true },
    { iso: 'IQ', code: '+964', flag: '🇮🇶', name: 'Iraq',        priority: true },
    { iso: 'SY', code: '+963', flag: '🇸🇾', name: 'Syria',       priority: true },
    { iso: 'PS', code: '+970', flag: '🇵🇸', name: 'Palestine',   priority: true },
    { iso: 'TR', code: '+90',  flag: '🇹🇷', name: 'Türkiye',     priority: true },
    { divider: true, label: '──────────' },
    { iso: 'US', code: '+1',   flag: '🇺🇸', name: 'United States' },
    { iso: 'CA', code: '+1',   flag: '🇨🇦', name: 'Canada' },
    { iso: 'GB', code: '+44',  flag: '🇬🇧', name: 'United Kingdom' },
    { iso: 'IE', code: '+353', flag: '🇮🇪', name: 'Ireland' },
    { iso: 'FR', code: '+33',  flag: '🇫🇷', name: 'France' },
    { iso: 'DE', code: '+49',  flag: '🇩🇪', name: 'Germany' },
    { iso: 'IT', code: '+39',  flag: '🇮🇹', name: 'Italy' },
    { iso: 'ES', code: '+34',  flag: '🇪🇸', name: 'Spain' },
    { iso: 'PT', code: '+351', flag: '🇵🇹', name: 'Portugal' },
    { iso: 'CH', code: '+41',  flag: '🇨🇭', name: 'Switzerland' },
    { iso: 'NL', code: '+31',  flag: '🇳🇱', name: 'Netherlands' },
    { iso: 'BE', code: '+32',  flag: '🇧🇪', name: 'Belgium' },
    { iso: 'AT', code: '+43',  flag: '🇦🇹', name: 'Austria' },
    { iso: 'SE', code: '+46',  flag: '🇸🇪', name: 'Sweden' },
    { iso: 'NO', code: '+47',  flag: '🇳🇴', name: 'Norway' },
    { iso: 'DK', code: '+45',  flag: '🇩🇰', name: 'Denmark' },
    { iso: 'FI', code: '+358', flag: '🇫🇮', name: 'Finland' },
    { iso: 'GR', code: '+30',  flag: '🇬🇷', name: 'Greece' },
    { iso: 'PL', code: '+48',  flag: '🇵🇱', name: 'Poland' }
  ];

  var DEFAULT_ISO = 'LB';
  var STORAGE_KEY = 'mk7_phone_cc_iso';
  var MIN_DIGITS = 7;
  var MAX_DIGITS = 15;

  function loadSavedIso() {
    try {
      var saved = localStorage.getItem(STORAGE_KEY);
      if (saved && COUNTRIES.some(function (c) { return c.iso === saved; })) return saved;
    } catch (e) { /* private mode etc. */ }
    return DEFAULT_ISO;
  }

  function saveIso(iso) {
    try { localStorage.setItem(STORAGE_KEY, iso); } catch (e) {}
  }

  function findCountry(iso) {
    for (var i = 0; i < COUNTRIES.length; i++) {
      if (COUNTRIES[i].iso === iso) return COUNTRIES[i];
    }
    return COUNTRIES[0];
  }

  // Strip non-digits AND a leading 0 (since users in many GCC/EU countries
  // write the local number with a leading 0, which is wrong for E.164).
  function cleanLocalDigits(raw) {
    var digits = (raw || '').replace(/\D/g, '');
    if (digits.charAt(0) === '0') digits = digits.slice(1);
    return digits;
  }

  function buildSelect(initialIso) {
    var sel = document.createElement('select');
    sel.className = 'phone-cc';
    sel.setAttribute('aria-label', 'Country code');

    for (var i = 0; i < COUNTRIES.length; i++) {
      var c = COUNTRIES[i];
      if (c.divider) {
        var opt = document.createElement('option');
        opt.disabled = true;
        opt.textContent = c.label || '──────────';
        sel.appendChild(opt);
        continue;
      }
      var o = document.createElement('option');
      o.value = c.iso;
      o.textContent = c.flag + ' ' + c.code;
      o.setAttribute('data-name', c.name);
      o.setAttribute('data-code', c.code);
      if (c.iso === initialIso) o.selected = true;
      sel.appendChild(o);
    }
    return sel;
  }

  function initOne(container) {
    if (container.__phoneInputInitialized) return;
    container.__phoneInputInitialized = true;

    var input = container.querySelector('input[type="tel"]');
    if (!input) return;

    container.classList.add('phone-input-group');

    var iso = loadSavedIso();
    var sel = buildSelect(iso);
    container.insertBefore(sel, input);

    // Stash the current iso/cc on the input so submit code can read it
    function syncToInput() {
      input.dataset.cc = sel.options[sel.selectedIndex].getAttribute('data-code') || '';
      input.dataset.iso = sel.value;
    }
    syncToInput();

    sel.addEventListener('change', function () {
      saveIso(sel.value);
      syncToInput();
      // Drop any error styling once they actively pick — give the form a fresh shot
      input.style.borderColor = '';
    });

    // Enforce digit-only typing on the local-number portion. Lets users still
    // paste formatted numbers; we'll clean on submit.
    input.addEventListener('input', function () {
      // only auto-clean if it's an obvious paste with junk — leave normal typing alone
      var v = input.value;
      if (/[^\d\s\-()]/.test(v)) {
        input.value = v.replace(/[^\d\s\-()]/g, '');
      }
    });
  }

  function initAll(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll('[data-phone-input]');
    for (var i = 0; i < nodes.length; i++) initOne(nodes[i]);
  }

  // Public helpers — used by the existing form-submit code in index.html and grow.html
  window.PhoneInput = {
    init: initAll,
    initOne: initOne,
    countries: COUNTRIES,

    // Returns the E.164-style number without the +, ready for wa.me links and CAPI
    // hashing. e.g. ("+961", "079 018 107") → "96179018107"
    digitsWithCountry: function (input) {
      if (!input) return '';
      var cc = (input.dataset.cc || '').replace(/\D/g, '');
      var local = cleanLocalDigits(input.value);
      if (!cc || !local) return '';
      return cc + local;
    },

    // Returns "+961xxxxxxx" with the leading + (useful for display)
    fullNumber: function (input) {
      var d = this.digitsWithCountry(input);
      return d ? '+' + d : '';
    },

    isValid: function (input) {
      var d = this.digitsWithCountry(input);
      var local = cleanLocalDigits(input ? input.value : '');
      return local.length >= MIN_DIGITS && d.length <= MAX_DIGITS;
    },

    // Lightweight error-message helper for inline validation feedback
    errorMessage: function (input) {
      if (!input) return 'Phone input not found';
      var local = cleanLocalDigits(input.value);
      if (!local) return 'Enter your WhatsApp number';
      if (local.length < MIN_DIGITS) return 'Number looks too short';
      if (local.length > MAX_DIGITS) return 'Number looks too long';
      return '';
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { initAll(); });
  } else {
    initAll();
  }
})();
