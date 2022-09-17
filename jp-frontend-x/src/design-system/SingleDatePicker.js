import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { SingleDatePicker as SingleDatePickerInput } from 'react-dates';
import system from '../utils/System';
import moment from 'moment';
import 'react-dates/lib/css/_datepicker.css';

import Box from './Box';
import Icon from './Icon';
import Select from './Select';
import Text from './Text';
import { toValidDate } from '../utils/date';
import i18n from '../i18n';

const getMonths = (date, minDate, maxDate) => {
  const minDateM = moment(minDate).startOf('month');
  const maxDateM = moment(maxDate).endOf('month');
  return [...Array(25)]
    .map((_, index) => index + date.month() - 12)
    .filter((month) => {
      return (
        (!minDate || !minDateM.isAfter(moment(date).set('month', month))) &&
        (!maxDate || !maxDateM.isBefore(moment(date).set('month', month)))
      );
    });
};

const isOutsideRange = (day, minDate, maxDate) => {
  const dayM = moment(moment(day).format('YYYY-MM-DD'));
  return dayM.isBefore(minDate) || dayM.isAfter(maxDate);
};

const SingleDatePickerContainer = system(
  'SingleDatePickerContainer',
  {
    extend: Box,
  },
  ({ theme, disabled }) => ({
    width: '100%',
    '.SingleDatePicker': {
      width: '100%',
    },
    '.SingleDatePickerInput': {
      display: 'flex',
      alignItems: 'center',
    },
    '.SingleDatePickerInput__disabled,.DateInput__disabled,.DateInput_input__disabled': {
      backgroundColor: 'transparent',
    },
    '.DateInput': {
      flex: 1,
    },
    '.CalendarDay': {
      border: 'none',
      color: theme.colors.on.surface[2],
    },
    '.DayPickerNavigation_button': {
      position: 'absolute',
      top: 20,
      '&:hover': {
        color: theme.colors.selects.primaryColor,
      },
      '&:first-of-type': {
        right: 70,
      },
      '&:last-of-type': {
        right: 22,
      },
    },
    '.DateInput_input': {
      fontSize: theme.fontSizes[4],
      width: '100%',
      padding: `${theme.space[1]} 0`,
      borderBottom: `${theme.borders[1]} ${
        disabled ? theme.colors.primaryGray : theme.colors.selects.primaryColor
      }`,
    },
    '.SingleDatePickerInput_calendarIcon': {
      padding: 0,
      margin: 0,
      marginLeft: -20,
      position: 'relative',
      cursor: disabled ? 'default' : 'pointer',
      color: disabled
        ? theme.colors.primaryGray
        : theme.colors.selects.primaryColor,
    },
    '.CalendarMonth_caption': {
      paddingTop: theme.space[3],
      fontSize: theme.fontSizes[4],
      fontWeight: theme.fontWeights[0],
    },
    '.DayPicker_weekHeader': {
      borderBottom: `${theme.borders[1]} ${theme.colors.primaryGray}`,
      top: 54,
      paddingBottom: `4px !important`,
    },
    '.CalendarDay__selected': {
      border: `${theme.borders[1]} ${theme.colors.selects.primaryColor}`,
      color: theme.colors.selects.primaryColor,
      backgroundColor: 'transparent',
    },
    '.DayPicker__withBorder': {
      borderRadius: 0,
    },
    '.SingleDatePicker_picker__directionLeft': {
      top: '34px !important',
    },
    '.CalendarDay__selected:hover': {
      background: theme.colors.selects.primaryColor,
      color: 'white',
    },
    '.CalendarDay__hovered_span:hover,.CalendarDay__hovered_span': {
      background: theme.colors.selects.primaryColor,
      color: 'white',
    },
  })
);

export const CalendarHeader = ({
  month: date,
  onMonthSelect,
  minDate,
  maxDate,
  onYearSelect,
}) => {
  return (
    <Select
      value={date.month()}
      border={0}
      onChange={(event) => onMonthSelect(date, Number(event.target.value))}
    >
      {getMonths(date, minDate, maxDate).map((month, index) => (
        <option value={month} key={index}>
          {moment(date)
            .set('month', month)
            .format('MMM YYYY')}
        </option>
      ))}
    </Select>
  );
};

class SingleDatePicker extends Component {
  state = {
    focusedInput: null,
  };

  handleFocusChange = ({ focused }) => {
    this.setState({ focused });
  };

  displayFormat = () => {
    const { formatDate, value } = this.props;
    if (typeof formatDate === 'undefined') {
      return undefined;
    } else if (typeof formatDate === 'function') {
      return formatDate(value);
    } else {
      return toValidDate(moment(value).toDate(), i18n.language);
    }
  };

  render() {
    const { focused } = this.state;
    const {
      id,
      label,
      onChange,
      placeholder,
      minDate,
      maxDate,
      value,
      disabled,
    } = this.props;
    return (
      <SingleDatePickerContainer disabled={disabled}>
        {label && <Text fontSize={4}>{label}</Text>}
        <SingleDatePickerInput
          onDateChange={onChange}
          onFocusChange={this.handleFocusChange}
          focused={focused}
          date={value}
          disabled={disabled}
          placeholder={placeholder}
          hideKeyboardShortcutsPanel
          noBorder
          id={id}
          numberOfMonths={1}
          navPrev={<Icon name="arrow-left" />}
          navNext={<Icon name="arrow-right" />}
          customInputIcon={<Icon name="calendar" size={20} />}
          inputIconPosition="after"
          verticalSpacing={0}
          isOutsideRange={(day) => isOutsideRange(day, minDate, maxDate)}
          renderMonthElement={(props) => (
            <CalendarHeader {...props} minDate={minDate} maxDate={maxDate} />
          )}
          displayFormat={this.displayFormat}
        />
      </SingleDatePickerContainer>
    );
  }
}

SingleDatePicker.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.node,
  maxDate: PropTypes.oneOfType([PropTypes.object, PropTypes.string]), // Date object or moment or ISO date string
  minDate: PropTypes.oneOfType([PropTypes.object, PropTypes.string]), // Date object or moment or ISO date string
  onChange: PropTypes.func,
  placeholder: PropTypes.string,
  id: PropTypes.string.isRequired,
  formatDate: PropTypes.oneOfType([PropTypes.func, PropTypes.string]),
  value: PropTypes.object, // Date object or moment or ISO date string
};

export default SingleDatePicker;
