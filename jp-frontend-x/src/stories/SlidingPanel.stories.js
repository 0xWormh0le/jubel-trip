/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import Button from '../design-system/Button';
import FlexPanel from '../design-system/FlexPanel';
import SlidingPanel from '../design-system/SlidingPanel';
import Text from '../design-system/Text';
import ToggleState from '../models/ToggleState';

const toggle = new ToggleState({ open: true });

storiesOf('SlidingPanel', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'default',
    () => (
      <SlidingPanel
        minWidth={80}
        maxWidth={800}
        snapWidths={[200, 400, 600]}
        resizable
      >
        <FlexPanel>
          <Text truncate m={4}>
            Here's the content.
          </Text>
          <Button mx={4} onClick={toggle.open}>
            Open sub-panel
          </Button>
        </FlexPanel>
        <SlidingPanel.DependentPanels>
          <SlidingPanel open={toggle.isOpen} onToggle={toggle.toggle}>
            <Text>Child panel</Text>
          </SlidingPanel>
        </SlidingPanel.DependentPanels>
      </SlidingPanel>
    ),
    {
      notes: {
        markdown: `
##### SlidingPanel Design Component

A design component sliding panel. You can pass the following props to the component.

- className: PropTypes.string,
- rtl: PropTypes.bool,
- maxWidth: numberString,
- minWidth: numberString,
- toggleOnHover: PropTypes.bool,
- toggleOnClick: PropTypes.bool,
- animated: PropTypes.bool,
- closeStyle: PropTypes.oneOf(['x', 'arrow', 'none']),
- closePosition: PropTypes.oneOf(['same', 'opposite']),
- open: PropTypes.bool,
- onOpen: PropTypes.func,
- onClose: PropTypes.func,
- resizeHandleIcon: PropTypes.string,
- resizable: PropTypes.bool, // if true, 'width' and 'onChangeWidth' props are required.
- width: PropTypes.number,
- snapWidths: PropTypes.array,
- onChangeWidth: PropTypes.func,

**Resolves**
[PR#66](https://github.com/Jubel-co/jp-frontend/pull/66)`,
      },
    }
  );
