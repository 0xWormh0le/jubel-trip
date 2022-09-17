/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import DisclosureButton from '../design-system/DisclosureButton';

storiesOf('DisclosureButton', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add('normal', () => <DisclosureButton name="arrow-right" />, {
    notes: {
      markdown: `
#### DisclosureButton Design Component

A design component for disclosure button

**Resolves**
[PR#45](https://github.com/Jubel-co/jp-frontend/pull/45)`,
    },
  })
  .add('primary', () => <DisclosureButton name="arrow-right" primary />)
  .add('disabled', () => <DisclosureButton name="arrow-right" disabled />);
