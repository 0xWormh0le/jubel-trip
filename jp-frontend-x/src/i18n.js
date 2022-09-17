import LanguageDetector from 'i18next-browser-languagedetector';
import XHR from 'i18next-xhr-backend';
import { currencyType } from './utils/currency';
import humanizeDuration from 'humanize-duration';
import { parse as parseDuration, toSeconds } from 'iso8601-duration';
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import moment from 'moment';

const formatCurrency = (amount, lng, currency) =>
  new Intl.NumberFormat(lng, {
    style: 'currency',
    currency: currency || currencyType[lng] || 'USD',
    maximumFractionDigits: 0,
    minimumFractionDigits: 0,
  }).format(amount);

const formatDuration = (value, lng, options, numberOnly = false) => {
  const duration = parseDuration(value);
  const ms = toSeconds(duration) * 1000;
  const language = lng.substr(0, 2);
  const result = humanizeDuration(ms, {
    language,
    largest: true,
    round: true,
    ...options,
  });
  return numberOnly ? result.replace(/\D/g, '') : result;
};

const formatList = (value, lng, options) => {
  return new Intl.ListFormat(lng, options).format(value);
};

i18n
  .use(XHR)
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    fallbackLng: 'en-US',
    debug: true,
    ns: ['common'],
    defaultNS: 'common',
    interpolation: {
      escapeValue: false,
      format: function(value, format, lng) {
        // console.log('i18n format', { value, format, lng });
        if (value instanceof Date) {
          let options = {};
          switch (format) {
            case 'MD':
              options = { year: 'numeric', month: '2-digit', day: '2-digit' };
              break;
            case 'MMM':
              options = { month: 'short' };
              break;
            case 'D':
              options = { day: 'numeric' };
              break;
            default:
              options = { year: 'numeric', month: '2-digit', day: '2-digit' };
              break;
          }
          return new Intl.DateTimeFormat(lng, options).format(value);
        } else if (format === 'currencyLocal')
          return formatCurrency(value, lng);
        else if (format === 'currencyUSD')
          return formatCurrency(value, lng, 'USD');
        else if (format === 'humanizeDuration')
          return formatDuration(value, lng);
        else if (format === 'durationDays')
          return formatDuration(value, lng, { units: ['d'] }, true);
        else if (format === 'listConjunction')
          return formatList(value, lng, {
            style: 'short',
            type: 'conjunction',
          });
        else if (format === 'listDisjunction')
          return formatList(value, lng, {
            style: 'short',
            type: 'disjunction',
          });
        else return new Intl.NumberFormat(lng).format(value);
      },
    },
    react: { wait: true },
  });

moment.updateLocale('en', { weekdaysMin: ['S', 'M', 'T', 'W', 'T', 'F', 'S'] });

export default i18n;
