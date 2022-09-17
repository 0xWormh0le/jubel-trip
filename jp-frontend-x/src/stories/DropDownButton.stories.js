/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';
import { withTranslation } from 'react-i18next';

import MaxPanel from '../design-system/MaxPanel';
import IconButton from '../design-system/IconButton';
import DropDownButton from '../design-system/DropDownButton';

const TranslatedDropDownButton = withTranslation('common')(DropDownButton);

const items = [
  {
    value: 'byName',
    label: 'by Name',
  },
  {
    value: 'byDate',
    label: 'by Date',
  },
  {
    value: 'byAge',
    label: 'by Age',
  },
];

const renderDotsButton = ({ iconSize, onClick, ref }) => (
  <IconButton
    onClick={onClick}
    size={iconSize}
    name="dots-vertical"
    px={1}
    ref={ref}
  />
);

const renderMenuItem = ({ label, selected }) => (
  <IconButton
    label={label}
    labelUppercase={false}
    labelBold={false}
    size={12}
    labelMargin={2}
    labelFontSize={4}
    name={selected ? 'check' : ''}
  />
);

storiesOf('DropDownButton', module)
  .addDecorator((story) => <MaxPanel centered>{story()}</MaxPanel>)
  .add('default', () => (
    <TranslatedDropDownButton
      label="Sort by"
      display="inline-block"
      uppercase={true}
      color="on.surface"
      value="byName"
      onSelect={action('onSelect')}
      items={items}
      menuMinWidth={120}
    />
  ))
  .add('custom renderer', () => (
    <TranslatedDropDownButton
      label="Sort by"
      display="inline-block"
      uppercase={true}
      color="on.surface"
      value="byName"
      onSelect={action('onSelect')}
      items={items}
      menuMinWidth={120}
      customButtonRenderer={renderDotsButton}
      customMenuItemRenderer={renderMenuItem}
    />
  ));
