import dfFormat from 'date-fns/format';
import dfIsAfter from 'date-fns/is_after';
import dfLastDayOfMonth from 'date-fns/last_day_of_month';
import dfSetDate from 'date-fns/set_date';
import dfSetMonth from 'date-fns/set_month';
import PropTypes from 'prop-types';
import React from 'react';
import DatePickerInput from 'react-datepicker';

import 'react-datepicker/dist/react-datepicker.css';

import system from '../utils/System';
import Box from './Box';
import Card from './Card';
import Flex from './Flex';
import Icon from './Icon';
import Select from './Select';
import Text from './Text';

const getMonths = (date, minDate, maxDate) =>
  [...Array(25)]
    .map((_, index) => index + date.getMonth() - 12)
    .filter(
      (month) =>
        (!minDate ||
          !dfIsAfter(dfSetDate(minDate, 1), dfSetMonth(date, month))) &&
        (!maxDate ||
          !dfIsAfter(dfSetMonth(date, month), dfLastDayOfMonth(maxDate)))
    );

const formatWeekDay = (value) => value.substring(0, 1);

const DatePickerContainer = system(
  'DatePickerContainer',
  {
    extend: Box,
  },
  (props) => ({
    width: '100%',
    '.react-datepicker-wrapper': {
      display: 'block',
    },
    '.react-datepicker__input-container': {
      display: 'block',
    },
    '.react-datepicker-popper': {
      marginTop: 0,
    },
  })
);

const NoBorderedInput = system(
  'NoBorderedInput',
  {
    extend: 'input',
  },
  ({ theme }) => ({
    border: 'none',
    fontSize: theme.fontSizes[5],
    padding: `${theme.space[2]} 0`,
    paddingRight: 24,
    marginRight: -24,
    backgroundColor: 'transparent',
    width: '100%',
    '&:disabled': {
      color: theme.colors.on.surface,
      '&::placeholder': {
        color: theme.colors.on.surface,
      },
    },
  })
);

const DateInputContainer = system(
  'DateInputContainer',
  {
    extend: Flex,
  },
  ({ theme, disabled }) => ({
    color: disabled
      ? theme.colors.primaryGray
      : theme.colors.selects.primaryColor,
    borderBottom: `${theme.borders[1]} ${
      disabled ? theme.colors.primaryGray : theme.colors.selects.primaryColor
    }`,
  })
);

const DateInput = React.forwardRef(({ forwardRef, ...props }, ref) => (
  <DateInputContainer
    alignItems="center"
    flexDirection="row-reverse"
    disabled={props.disabled}
  >
    <Icon name="calendar" color="inherit" />
    <Flex flex={1}>
      <NoBorderedInput {...props} ref={ref} />
    </Flex>
  </DateInputContainer>
));

const CalendarHeaderContainer = system(
  'CalendarHeaderContainer',
  {
    extend: Flex,
  },
  ({ theme }) => ({
    padding: theme.space[4],
  })
);

const NavButton = system(
  'NavButton',
  {
    extend: Box,
  },
  ({ theme, disabled }) => ({
    ...(disabled
      ? {
          color: theme.colors.primaryGray,
        }
      : {
          cursor: 'pointer',
          '&:hover': {
            color: theme.colors.selects.primaryColor,
          },
        }),
  })
);

const CalendarHeader = ({
  date,
  changeYear,
  changeMonth,
  decreaseMonth,
  increaseMonth,
  prevMonthButtonDisabled,
  nextMonthButtonDisabled,
  minDate,
  maxDate,
}) => {
  return (
    <CalendarHeaderContainer alignItems="center">
      <Flex flex={1}>
        <Select
          value={date.getMonth()}
          border={0}
          onChange={(event) => changeMonth(event.target.value)}
        >
          {getMonths(date, minDate, maxDate).map((month, index) => (
            <option value={month} key={index}>
              {dfFormat(dfSetMonth(date, month), 'MMM YYYY')}
            </option>
          ))}
        </Select>
      </Flex>
      <Flex>
        <NavButton
          disabled={prevMonthButtonDisabled}
          tabIndex={0}
          onClick={prevMonthButtonDisabled ? undefined : decreaseMonth}
        >
          <Icon name="arrow-left" />
        </NavButton>
      </Flex>
      <Box width={16} />
      <Flex>
        <NavButton
          disabled={prevMonthButtonDisabled}
          tabIndex={0}
          onClick={nextMonthButtonDisabled ? undefined : increaseMonth}
        >
          <Icon name="arrow-right" />
        </NavButton>
      </Flex>
    </CalendarHeaderContainer>
  );
};

const CalendarContainer = system(
  'CalendarContainer',
  {
    extend: Card,
    boxShadow: 2,
  },
  ({ theme }) => ({
    color: theme.colors.on.surface,
    border: 'none',
    borderRadius: 0,
    '.react-datepicker__header--custom': {
      padding: 0,
      borderRadius: 0,
      background: 'transparent',
      borderBottomColor: '#e9e9e9',
    },
    '.react-datepicker__day--outside-month': {
      opacity: 0.5,
    },
    '.react-datepicker__day': {
      boxSizing: 'border-box',
      border: `${theme.borders[1]} transparent`,
      color: 'inherit',
      height: '2rem',
      '&:hover': {
        borderRadius: 0,
      },
    },
    '.react-datepicker__day-name': {
      fontSize: theme.fontSizes[2],
      color: theme.colors.on.surface,
      lineHeight: 1,
      paddingBottom: theme.space[1],
    },
    '.react-datepicker__day,.react-datepicker__day-name': {
      width: '2rem',
      margin: theme.space[1],
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
    '.react-datepicker__day--keyboard-selected,.react-datepicker__day--keyboard-selected:hover,.react-datepicker__day--selected,.react-datepicker__day--selected:hover': {
      borderRadius: 0,
      border: `${theme.borders[1]} ${theme.colors.selects.primaryColor}`,
      backgroundColor: 'transparent',
      color: '#6A8FFE',
    },
    '.react-datepicker__day--disabled': {
      color: theme.colors.primaryGray,
    },
    '.react-datepicker__month': {
      marginBottom: theme.space[2],
    },
  })
);

const DatePicker = ({
  disabled,
  formatDate,
  label,
  maxDate,
  minDate,
  onChange,
  placeholder,
  value,
}) => (
  <DatePickerContainer>
    {label && (
      <Text disabled={disabled} fontSize={4}>
        {label}
      </Text>
    )}
    <DatePickerInput
      customInput={<DateInput />}
      calendarContainer={(props) => <CalendarContainer {...props} />}
      renderCustomHeader={(props) => (
        <CalendarHeader {...props} minDate={minDate} maxDate={maxDate} />
      )}
      formatWeekDay={formatWeekDay}
      showMonthDropdown
      useShortMonthInDropdown
      popperModifiers={{
        preventOverflow: {
          enabled: true,
          escapeWithReference: false, // force popper to stay in viewport (even when input is scrolled out of view)
          boundariesElement: 'viewport',
        },
      }}
      disabled={disabled}
      placeholderText={placeholder}
      formatDate={formatDate}
      onChange={onChange}
      selected={value}
      minDate={minDate}
      maxDate={maxDate}
    />
  </DatePickerContainer>
);

DatePicker.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.node,
  maxDate: PropTypes.object, // Date object or undefined
  minDate: PropTypes.object, // Date object or undefined
  onChange: PropTypes.func,
  placeholder: PropTypes.string,
  value: PropTypes.object, // Date object
};

export default DatePicker;
