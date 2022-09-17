/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import VideoPlayer from '../design-system/VideoPlayer';

storiesOf('VideoPlayer', module)
  .addDecorator((story) => (
    <Box mx="auto" width={600} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'default',
    () => (
      <VideoPlayer
        src="https://www.youtube.com/embed/0LuiUtv2kug"
        height={320}
        width={480}
      />
    ),
    {
      notes: {
        markdown: `
      #### VideoPlayer Design Component

      A design component for video player

      **Resolves**
      [PR#74](https://github.com/Jubel-co/jp/pull/74)`,
      },
    }
  );
