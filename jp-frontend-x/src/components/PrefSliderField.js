import React, { useCallback, useState } from 'react';
import { withTranslation } from 'react-i18next';

import PrefRow from './PrefRow';
import Absolute from '../design-system/Absolute';
import Box from '../design-system/Box';
import SimpleSlider from '../design-system/SimpleSlider';
import ToggleButton from '../design-system/ToggleButton';

const PrefSliderField = ({
  label,
  value,
  onChange,
  updateSwitches,
  switchName,
  switchValue = false,
  valueFormatter,
  tkey,
  info,
  sliderColor,
  tipType,
  t,
}) => {
  const [cachedValue, setCachedValue] = useState(value);

  const handleChangeSwitch = useCallback(
    (val) => {
      updateSwitches &&
        updateSwitches({
          [switchName]: Boolean(val),
        });
    },
    [updateSwitches, switchName]
  );

  const handleChangeSlider = useCallback(
    (val) => {
      onChange(val);
      if (!switchValue) {
        updateSwitches({ [switchName]: true });
      }
    },
    [onChange, updateSwitches, switchName, switchValue]
  );

  return (
    <PrefRow
      tkey={tkey}
      label={label}
      value={
        switchValue
          ? valueFormatter(cachedValue, t)
          : t('indifferent', 'Indifferent')
      }
      info={info}
      tipType={tipType}
    >
      <Box mt={2}>
        <SimpleSlider
          value={value}
          onUpdate={setCachedValue}
          onChange={handleChangeSlider}
          color={sliderColor}
          disabled={!switchValue}
        />
      </Box>
      <Absolute right={0} top={0} mt={2} mr={3}>
        <ToggleButton
          name={switchName}
          checked={switchValue}
          onChange={handleChangeSwitch}
          size={24}
        />
      </Absolute>
    </PrefRow>
  );
};

export default withTranslation('common')(PrefSliderField);
