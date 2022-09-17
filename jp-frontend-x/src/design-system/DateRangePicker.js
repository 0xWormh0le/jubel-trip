import { DateRangePicker as DateRangePickerInput } from 'react-dates';
import get from 'lodash/get';
import moment from 'moment';
import PropTypes from 'prop-types';
import React, { Component } from 'react';

import 'react-dates/lib/css/_datepicker.css';

import { toValidDate } from '../utils/date';
import Box from './Box';
import Flex from './Flex';
import i18n from '../i18n';
import Icon from './Icon';
import Select from './Select';
import system from '../utils/System';
import Text from './Text';

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
  return (
    (minDate && dayM.isBefore(minDate)) || (maxDate && dayM.isAfter(maxDate))
  );
};

const DateRangePickerContainer = system(
  'DateRangePicker',
  {
    extend: Box,
  },
  ({ theme, disabled, fixed }) => ({
    width: '100%',
    color: disabled
      ? theme.colors.primaryGray
      : theme.colors.selects.primaryColor,
    '.DateRangePicker': {
      width: '100%',
    },
    '.DateRangePickerInput': {
      display: 'flex',
      alignItems: 'center',
    },
    '.DateRangePickerInput__disabled,.DateInput__disabled,.DateInput_input__disabled': {
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
        left: 22,
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
      '&:disabled': {
        color: theme.colors.on.surface[0],
        '&::placeholder': {
          color: theme.colors.on.surface[0],
        },
      },
    },
    '.DateRangePickerInput_arrow,.DateRangePickerInput_calendarIcon': {
      padding: 0,
      margin: 0,
      marginLeft: -20,
      position: 'relative',
      cursor: disabled ? 'default' : 'pointer',
      color: disabled
        ? theme.colors.primaryGray
        : theme.colors.selects.primaryColor,
    },
    '.DateRangePickerInput_arrow': {
      marginRight: 24,
      position: 'relative',
      '&:after': {
        position: 'absolute',
        content: '"—"',
        left: '100%',
        top: 0,
        width: 24,
        height: 24,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      },
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
    '.CalendarDay__selected_start': {
      background: theme.colors.selects.primaryColor,
      border: 'none',
      color: 'white', //text
    },
    '.CalendarDay__selected_span': {
      color: 'white', //text
      background: theme.colors.selects.primaryColor,
      border: 'none',
    },
    '.CalendarDay__selected_end': {
      color: 'white', //text
      background: theme.colors.selects.primaryColor,
      border: 'none',
    },
    '.CalendarDay__selected': {
      background: theme.colors.selects.primaryColor,
      border: 'none',
      color: 'white',
    },
    '.DayPicker__withBorder': {
      borderRadius: 0,
      position: 'fixed',
      right: fixed && 30,
    },
    '.DateRangePicker_picker__directionLeft': {
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
    <Flex justifyContent="center">
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
    </Flex>
  );
};

class DateRangePicker extends Component {
  state = {
    focusedInput: null,
  };

  handleDatesChange = ({ startDate, endDate }) => {
    const { onChange, value } = this.props;
    onChange({
      startDate: startDate || get(value, startDate, null),
      endDate: endDate || get(value, 'endDate', null),
    });
  };

  handleFocusChange = (focusedInput) => {
    this.setState({ focusedInput });
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
    const { focusedInput } = this.state;
    const {
      startPlaceholder = 'MM/DD/YYYY',
      endPlaceholder = 'MM/DD/YYYY',
      startLabel,
      endLabel,
      minDate,
      maxDate,
      value,
      disabled,
      startDateId,
      endDateId,
      fixed,
    } = this.props;
    return (
      <DateRangePickerContainer disabled={disabled} fixed={fixed}>
        {startLabel || endLabel ? (
          <Flex fontSize={4}>
            <Flex flex={1}>
              <Text>{startLabel}</Text>
            </Flex>
            <Box width={24} />
            <Flex flex={1}>
              <Text>{endLabel}</Text>
            </Flex>
          </Flex>
        ) : null}
        <DateRangePickerInput
          onDatesChange={this.handleDatesChange}
          onFocusChange={this.handleFocusChange}
          focusedInput={focusedInput}
          startDate={get(value, 'startDate') || null}
          endDate={get(value, 'endDate') || null}
          disabled={disabled}
          startDatePlaceholderText={startPlaceholder || ''}
          endDatePlaceholderText={endPlaceholder || ''}
          startDateId={startDateId}
          endDateId={endDateId}
          hideKeyboardShortcutsPanel
          noBorder
          daySize={36}
          navPrev={<Icon name="arrow-left" />}
          navNext={<Icon name="arrow-right" />}
          customInputIcon={<Icon name="calendar" size={20} />}
          customArrowIcon={<Icon name="calendar" size={20} />}
          inputIconPosition="after"
          verticalSpacing={0}
          isOutsideRange={(day) => isOutsideRange(day, minDate, maxDate)}
          renderMonthElement={(props) => (
            <CalendarHeader {...props} minDate={minDate} maxDate={maxDate} />
          )}
          displayFormat={this.displayFormat}
        />
      </DateRangePickerContainer>
    );
  }
}

DateRangePicker.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.node,
  maxDate: PropTypes.oneOfType([PropTypes.object, PropTypes.string]), // Date object or moment or ISO date string
  minDate: PropTypes.oneOfType([PropTypes.object, PropTypes.string]), // Date object or moment or ISO date string
  onChange: PropTypes.func,
  startLabel: PropTypes.string,
  endLabel: PropTypes.string,
  startPlaceholder: PropTypes.string,
  endPlaceholder: PropTypes.string,
  startDateId: PropTypes.string.isRequired,
  endDateId: PropTypes.string.isRequired,
  formatDate: PropTypes.oneOfType([PropTypes.func, PropTypes.string]),
  value: PropTypes.object, // Date object or moment or ISO date string
};

export default DateRangePicker;
