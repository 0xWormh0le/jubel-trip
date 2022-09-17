/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import TabButton from '../design-system/TabButton';

const tabs = [
  {
    id: 'tab1',
    name: 'Tab 1',
    active: true,
  },
  {
    id: 'tab2',
    name: 'Tab 2',
  },
  {
    id: 'tab3',
    name: 'Tab3',
  },
];

storiesOf('TabButton', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'normal',
    () => (
      <Box p={3}>
        {tabs.map((tab) => (
          <TabButton
            key={tab.id}
            active={tab.active}
            onClick={action('onClick')}
          >
            {tab.name}
          </TabButton>
        ))}
      </Box>
    ),
    {
      notes: {
        markdown: `
      #### TabButton Design Component

      A design component for tab button. When tab is active, there is as default
      green border Bottom. borderColor can be passed as argument
      
      **Resolves**
      [PR#101](https://github.com/Jubel-co/jp/pull/101)`,
      },
    }
  );
