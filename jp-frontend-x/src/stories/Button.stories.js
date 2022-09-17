/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';
import { Button, MaxPanel } from '../design-system';

storiesOf('Button', module)
  .addDecorator((story) => <MaxPanel centered>{story()}</MaxPanel>)
  .add('normal no bg', () => <Button>Normal - Hover</Button>)
  .add(
    'normal hoverable',
    () => (
      <Button bg="surface" onClick={action('button clicked')}>
        Normal + Hover
      </Button>
    ),
    {
      notes: `
      A **normal** button with a click action.
      
      Note: buttons are \`hoverable\` by default, 
      but require a \`bg\` to be set in order for the hover to work.`,
    }
  )
  .add('active', () => <Button active>Active</Button>, {
    notes: 'A **active** button.',
  })
  .add('primary', () => <Button primary>Primary</Button>, {
    notes: 'A **primary** button.',
  })
  .add('secondary', () => <Button secondary>Secondary</Button>, {
    notes: 'A **secondary** button.',
  })
  .add('info', () => <Button info>Info</Button>, {
    notes: 'An **info** button.',
  })
  .add('error', () => <Button error>Error</Button>, {
    notes: 'An **error** button.',
  })
  .add('warning', () => <Button warning>Warning</Button>, {
    notes: 'A **warning**.',
  })
  .add('disabled', () => <Button disabled>Disabled</Button>, {
    notes: 'A **disabled** button.',
  });
