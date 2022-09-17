import PropTypes from 'prop-types';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { Text, Button } from '../design-system';
import { numberString } from '../utils/propTypes';
import system from '../utils/System';

export const TravelStyleButtonBase = system(
  'TravelStyleButtonBase',
  {
    extend: Button,
    fontWeight: 'bold',
    px: 0,
    py: 0,
    width: 60,
  },
  ({ width, height, minWidth }) => ({
    minWidth: Math.min(minWidth, width),
    height: height || width,
  })
);

const TravelStyleButton = ({
  travelStyle,
  children,
  disabled,
  tooltipEnabled,
  t,
  ...otherProps
}) => (
  <TravelStyleButtonBase
    color={travelStyle.onColor}
    bg={travelStyle.color}
    borderColor={travelStyle.borderColor}
    disabled={disabled}
    data-rh={
      tooltipEnabled
        ? t(`styles_${travelStyle.abbr}_name`, travelStyle.name)
        : undefined
    }
    title={
      tooltipEnabled
        ? undefined
        : t(`styles_${travelStyle.abbr}_name`, travelStyle.name)
    }
    {...otherProps}
  >
    {children || (
      <Text uppercase>
        {t(`styles_${travelStyle.abbr}_abbr`, travelStyle.abbr)}
      </Text>
    )}
  </TravelStyleButtonBase>
);

TravelStyleButton.propTypes = {
  travelStyle: PropTypes.object.isRequired,
  children: PropTypes.node,
  disabled: PropTypes.bool,
  width: numberString,
  height: numberString,
};

export default withTranslation('common')(TravelStyleButton);
