/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import { stripMargin } from '../utils/strings';
import { TooltipWrapper } from '../components';
import Box from '../design-system/Box';
import Button from '../design-system/Button';

storiesOf('Tooltip', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      <TooltipWrapper />
      {story()}
    </Box>
  ))
  .add('simple', () => <Button data-rh="Tooltip text">Simple tooltip</Button>, {
    notes: {
      markdown: `
#### Tooltip

Specify data-rh prop to have a tooltip on the component.

**Resolves**
[PR#106](https://github.com/Jubel-co/jp/issues/177)`,
    },
  })
  .add('with markdown', () => (
    <Button
      data-rh-markdown={stripMargin(`
          #### Tooltip using Markdown

          Specify data-rh-markdown prop to have a markdown tooltip on the component.
        `)}
      data-rh-markdown-at="bottom"
    >
      Tooltip with Markdown
    </Button>
  ));
