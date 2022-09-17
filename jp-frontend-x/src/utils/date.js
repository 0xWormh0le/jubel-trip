import humanizeDuration from 'humanize-duration';
import { parse as parseDuration, toSeconds } from 'iso8601-duration';
import { isBefore } from 'date-fns';

export const toValidDate = (date, lng) => {
  return new Intl.DateTimeFormat(lng).format(date);
};

export const durationToDays = (duration) =>
  parseInt(
    humanizeDuration(toSeconds(parseDuration(duration)) * 1000, {
      units: ['d'],
    }).split(' ')[0],
    10
  );

export const dfMin = (date1, date2) => (isBefore(date1, date2) ? date1 : date2);

export const dfMax = (date1, date2) => (isBefore(date1, date2) ? date2 : date1);
