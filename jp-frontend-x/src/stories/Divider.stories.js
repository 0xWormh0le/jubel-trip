/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import Divider from '../design-system/Divider';
import Relative from '../design-system/Relative';
import Text from '../design-system/Text';

storiesOf('Divider', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      <Text>Before Stuff</Text>
      {story()}
      <Text>After Stuff</Text>
    </Box>
  ))
  .add('Horizontal', () => <Divider text="OR" width={1} bg="surface" />, {
    notes: {
      markdown: `
#### Divider Design Component

A design component to use vertical and horizontal divider
with or without text in the middle

**Resolves**
[PR#102](https://github.com/Jubel-co/jp/pull/102)`,
    },
  })
  .add('Vertical', () => (
    <Divider borderColor="primary" vertical thickness={2} height={150}>
      <Relative p={2} borderRadius="sm" primary uppercase>
        then
      </Relative>
    </Divider>
  ));
