import React, { useCallback, useState, useEffect, useRef } from 'react';
import {
  addDays,
  differenceInCalendarDays,
  format,
  parse,
  isValid,
} from 'date-fns';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import DayPicker from 'react-day-picker';

import { Button, Flex, IconButton, Input, Modal, Text } from '../design-system';
import colors from '../design-system/theme/colors';

import 'react-day-picker/lib/style.css';

const FOCUS_RING = {
  none: 0,
  from: 1,
  to: 2,
};

const pickerColors = colors.misc.datePicker;

const today = new Date();

const px = (n) => `${n}px`;

const formatDate = (dt) => (dt ? format(dt, 'MM/DD/YYYY') : '');

const isBefore = (left, right) => differenceInCalendarDays(left, right) < 0;

const isAfter = (left, right) => differenceInCalendarDays(left, right) > 0;

const isEqual = (left, right) => differenceInCalendarDays(left, right) === 0;

const CalendarWidget = ({
  isOpen,
  departOnOrAfterDate,
  returnOnOrBeforeDate,
  clearDateRange,
  dateRangeStrings,
  position,
  setDateRange,
  closePicker,
  t,
}) => {
  const initFocusRing = () => {
    if (departOnOrAfterDate === null && returnOnOrBeforeDate === null) {
      return FOCUS_RING.from;
    } else if (departOnOrAfterDate && returnOnOrBeforeDate) {
      return FOCUS_RING.none;
    } else {
      return FOCUS_RING.to;
    }
  };

  const [dateStart, setDateStart] = useState(departOnOrAfterDate || null);
  const [dateEnd, setDateEnd] = useState(returnOnOrBeforeDate || null);

  const [focusRing, setFocusRing] = useState(initFocusRing());
  const fromInput = useRef(null);
  const toInput = useRef(null);

  const [dateEnter, setDateEnter] = useState(null);
  const [dateHover, setDateHover] = useState(null);

  const [dateFromInput, setDateFromInput] = useState('');
  const [dateToInput, setDateToInput] = useState('');

  const getDateBetweenRange = useCallback(() => {
    const from = dateStart;
    const to = dateEnd || dateEnter || dateStart;
    const duration = differenceInCalendarDays(to, from);
    if (from && duration > 1) {
      return {
        from: addDays(from, 1),
        to: addDays(to, -1),
      };
    }
    return null;
  }, [dateStart, dateEnd, dateEnter]);

  const handleDayMouseEnter = useCallback(
    (day) => {
      setDateHover(day);
      if (
        dateStart &&
        dateEnd === null &&
        focusRing === FOCUS_RING.to &&
        !isBefore(day, dateStart)
      ) {
        setDateEnter(day);
      } else {
        setDateEnter(null);
      }
    },
    [dateStart, dateEnd, focusRing]
  );

  const handleDayClick = useCallback(
    (day) => {
      if (isBefore(day, addDays(today, 1))) {
        return;
      }

      if (dateStart === null && dateEnd === null) {
        setDateStart(day);
        setFocusRing(FOCUS_RING.to);
      } else if (dateEnd === null) {
        if (focusRing === FOCUS_RING.to && !isBefore(day, dateStart)) {
          setDateEnd(day);
          setFocusRing(FOCUS_RING.none);
        } else {
          setDateStart(day);
          setFocusRing(FOCUS_RING.to);
        }
      } else {
        if (focusRing === FOCUS_RING.to) {
          if (isBefore(day, dateStart)) {
            setDateStart(day);
            setFocusRing(FOCUS_RING.to);
          } else {
            setDateEnd(day);
            setFocusRing(FOCUS_RING.none);
          }
        } else {
          if (!isBefore(day, dateEnd)) {
            setDateEnd(null);
            setDateEnter(null);
          }
          setDateStart(day);
          setFocusRing(FOCUS_RING.to);
        }
      }
    },
    [dateStart, dateEnd, focusRing]
  );

  useEffect(() => {
    if (focusRing === FOCUS_RING.from) {
      fromInput.current && fromInput.current.focus();
    } else if (focusRing === FOCUS_RING.to) {
      toInput.current && toInput.current.focus();
    } else {
      fromInput.current && fromInput.current.blur();
      toInput.current && toInput.current.blur();
    }
  }, [focusRing]);

  useEffect(() => {
    if (dateStart && dateEnd) {
      setDateRange(dateStart, dateEnd);
    }

    if (dateStart === null && dateEnd === null) {
      setFocusRing(FOCUS_RING.from);
    }

    setDateFromInput(formatDate(dateStart));
    setDateToInput(formatDate(dateEnd));
  }, [dateStart, dateEnd, setDateRange]);

  useEffect(() => {
    setDateStart(departOnOrAfterDate);
    setDateEnd(returnOnOrBeforeDate);
  }, [departOnOrAfterDate, returnOnOrBeforeDate]);

  const isCircle = useCallback(() => {
    if (dateStart && !dateEnd) {
      if (!dateEnter || isEqual(dateStart, dateEnter)) {
        return true;
      }
    }
    return dateStart && isEqual(dateStart, dateEnd);
  }, [dateStart, dateEnd, dateEnter]);

  const hoverDateStyle = useCallback(() => {
    if (isBefore(dateHover, dateStart) || isAfter(dateHover, dateEnd)) {
      if (!isEqual(dateHover, dateEnter) && !isEqual(dateHover, dateStart)) {
        return {
          boxShadow: 'inset 0 0 0px 2px lightgray',
        };
      }
    }
    return { opacity: 0.5 };
  }, [dateHover, dateEnter, dateStart, dateEnd]);

  const handleFromInputChange = (val) => setDateFromInput(val);

  const handleToInputChange = (val) => setDateToInput(val);

  const handleInputEnter = (event, type) => {
    if (event.type === 'keydown' && event.which === 13) {
      const dt = parse(event.target.value);

      if (isValid(dt) && isAfter(dt, today)) {
        handleDayClick(dt);
      } else {
        if (type === 'from') {
          setDateFromInput(formatDate(dateStart));
        } else {
          setDateToInput(formatDate(dateEnd));
        }
      }
    }
  };

  const handleClosePicker = () => {
    if (!(dateStart && dateEnd)) {
      setDateStart(null);
      setDateEnd(null);
    }
    closePicker();
  };

  const circle = isCircle();

  return (
    <Modal
      open={isOpen}
      position={position}
      fontSize={4}
      close={handleClosePicker}
    >
      <Flex
        p={px(position.inputOffset)}
        alignItems="center"
        justifyContent="space-between"
        bg="background"
        borderBottom={1}
      >
        <Text bold uppercase textAlign="center">
          {dateRangeStrings.days ? t('days_nights', dateRangeStrings) : ''}
        </Text>
        <Flex flex={1} ml={2}>
          <Input
            width={120}
            mr={2}
            value={dateFromInput}
            ref={fromInput}
            onChange={handleFromInputChange}
            onFocus={() => setFocusRing(FOCUS_RING.from)}
            onKeyDown={(e) => handleInputEnter(e, 'from')}
            className={focusRing === FOCUS_RING.from && 'focus-visible'}
            placeholder={t('date_picker_departure_placeholder', 'Departure')}
          />
          <Input
            width={120}
            value={dateToInput}
            ref={toInput}
            onChange={handleToInputChange}
            onFocus={() => setFocusRing(FOCUS_RING.to)}
            onKeyDown={(e) => handleInputEnter(e, 'to')}
            className={focusRing === FOCUS_RING.to && 'focus-visible'}
            placeholder={t('date_picker_return_placeholder', 'Return')}
          />
        </Flex>
      </Flex>
      <DayPicker
        styles={{ fontSize: '.6rem' }}
        fromMonth={today}
        initialMonth={departOnOrAfterDate || today}
        enableOutsideDaysClick
        showOutsideDays
        numberOfMonths={2}
        modifiers={{
          disabled: { before: today },
          departOnOrAfterDate: dateStart,
          returnOnOrBeforeDate: dateEnd,
          enterDate: dateStart && !dateEnd && dateEnter,
          selected: getDateBetweenRange(),
          hoverDate: dateHover,
        }}
        modifiersStyles={{
          departOnOrAfterDate: {
            color: pickerColors.rangeEndDays.color,
            backgroundColor: pickerColors.rangeEndDays.bg,
            borderRadius: circle ? '50%' : '50% 0 0 50%',
          },
          returnOnOrBeforeDate: {
            color: pickerColors.rangeEndDays.color,
            backgroundColor: pickerColors.rangeEndDays.bg,
            borderRadius: circle ? '50%' : '0 50% 50% 0',
          },
          enterDate: {
            color: pickerColors.rangeEndDays.color,
            backgroundColor: pickerColors.rangeEndDays.bg,
            borderRadius: circle ? '50%' : '0 50% 50% 0',
          },
          hoverDate: hoverDateStyle(),
          selected: {
            color: pickerColors.rangeInsideDays.color,
            backgroundColor: pickerColors.rangeInsideDays.bg,
            borderRadius: '0',
          },
          today: {
            color: pickerColors.today.color,
            backgroundColor: pickerColors.today.bg,
            fontWeight: 'normal',
            borderRadius: '50%',
          },
        }}
        onDayMouseEnter={handleDayMouseEnter}
        onDayClick={handleDayClick}
      />
      <Flex
        alignItems="center"
        justifyContent="flex-end"
        p={px(position.inputOffset)}
        bg="background"
        borderTop={1}
      >
        <Button
          disabled={!departOnOrAfterDate && !returnOnOrBeforeDate}
          onClick={clearDateRange}
          py={2}
          mr={3}
          bg="surface"
        >
          {t('date_picker_clear_dates', 'Clear Dates')}
        </Button>
        <Button py={2} bg="surface" onClick={handleClosePicker}>
          {t('close')}
        </Button>
      </Flex>
    </Modal>
  );
};

export const TravelDatesPicker = ({
  isOpen,
  placeholder,
  setDateRange,
  departOnOrAfterDate,
  returnOnOrBeforeDate,
  clearDateRange,
  dateRangeStrings,
  openPicker,
  closePicker,
  position,
  t,
}) => (
  <>
    <IconButton
      labelUppercase={false}
      labelBold={false}
      labelFontSize={4}
      border={2}
      borderRadius="sm"
      bg="surface"
      hoverable={false}
      p={2}
      name="calendar"
      labelPosition="left"
      justifyContent="space-between"
      placeholder={placeholder}
      label={
        dateRangeStrings.days
          ? t('days_range', dateRangeStrings)
          : t('date_picker_empty_range', 'When are you traveling?')
      }
      onClick={openPicker}
    />
    <CalendarWidget
      isOpen={isOpen}
      departOnOrAfterDate={departOnOrAfterDate}
      returnOnOrBeforeDate={returnOnOrBeforeDate}
      dateRangeStrings={dateRangeStrings}
      setDateRange={setDateRange}
      closePicker={closePicker}
      position={position}
      clearDateRange={clearDateRange}
      t={t}
    />
  </>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    isOpen: app.preferences.datesPickerOpen,
    position: app.preferences.datesPickerPosition,
    openPicker: (event) => app.preferences.openDatesPicker(event.currentTarget),
    closePicker: () => app.preferences.closeDatesPicker(),
    setDateRange: app.preferences.setDateRange,
    departOnOrAfterDate: app.preferences.departOnOrAfterDate,
    returnOnOrBeforeDate: app.preferences.returnOnOrBeforeDate,
    dateRangeStrings: app.preferences.dateRangeStrings,
    clearDateRange: app.preferences.clearDateRange,
  })),
  observer
)(TravelDatesPicker);
