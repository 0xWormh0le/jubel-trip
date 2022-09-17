/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import Text from '../design-system/Text';

storiesOf('Text', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add('upercase', () => <Text uppercase>uppercase</Text>, {
    notes: {
      markdown: `
      #### TextStory Design Component

      We have text styles for _uppercase_, _lowercase_, _capitalize_, _truncate_ for the Text component.

      **Resolves**
      [PR#106](https://github.com/Jubel-co/jp/pull/106)`,
    },
  })
  .add('lowercase', () => <Text textStyle="lowercase">LOWERCASE</Text>)
  .add('capitalize', () => (
    <Text textStyle="capitalize">this should be capitalized</Text>
  ))
  .add('truncate', () => (
    <Text textStyle="truncate" style={{ maxWidth: '5em' }}>
      truncated long
    </Text>
  ));
