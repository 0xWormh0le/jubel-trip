import React, { useEffect, useRef, useState } from 'react';
import { themeGet } from 'styled-system';
import { withTranslation } from 'react-i18next';
import { Manager, Reference, Popper } from 'react-popper';

import Box from './Box';
import Hr from './Hr';
import IconButton from './IconButton';
import Relative from './Relative';
import system from '../utils/System';
import Text from './Text';

export const MenuList = system('MenuList', {
  extend: Box,
  boxShadow: 2,
  overflow: 'hidden',
  bg: 'surface',
  zIndex: 10,
  borderRadius: 'sm',
});

export const ListItem = system(
  'ListItem',
  {
    extend: Text,
    px: 3,
    py: 3,
    width: 1,
    cursor: 'default',
  },
  (props) =>
    props.disabled
      ? {
          color: themeGet('colors.on.disabled')(props),
        }
      : {
          '&:hover': {
            backgroundColor: themeGet('colors.active')(props),
          },
        }
);

const useOutsideHandler = (ref, callback) => {
  const handleClickOutside = (event) => {
    if (ref.current && !ref.current.contains(event.target)) {
      callback();
    }
  };

  useEffect(() => {
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  });
};

export const DropDownButton = ({
  label,
  items,
  value,
  onSelect,
  uppercase = false,
  iconSize = 28,
  t,
  customButtonRenderer,
  customMenuItemRenderer,
  menuMinWidth,
  placement = 'auto-end',
  ...props
}) => {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef(null);
  const activeItem = items.find((item) => item.value === value) || {};
  const [focusedIndex, setFocusedIndex] = useState(-1);

  useOutsideHandler(wrapperRef, () => setOpen(false));

  const navigateDropDown = (event, items) => {
    let key = event.keyCode;
    let newIndex = 0;
    let item = items.find((item, index) => index === focusedIndex);
    switch (key) {
      // press enter
      case 13:
        if (item && !item.disabled) {
          handleToggleAndSelect(item);
        }
        return;
      // press ESC
      case 27:
        return setOpen(false);
      // press ARROW UP
      case 40:
        if (focusedIndex === items.length) {
          setFocusedIndex(0);
          return;
        }
        newIndex = focusedIndex + 1;
        break;
      // press ARROW UP
      case 38:
        if (focusedIndex === 0) {
          setFocusedIndex(items.length);
          return;
        }
        newIndex = focusedIndex - 1;
        break;
      default:
        break;
    }
    setFocusedIndex(newIndex);
  };

  const handleToggleAndSelect = (item, type = 'navigationClick') => {
    onSelect && onSelect(item);
    if (type !== 'navigationClick') {
      setOpen(false);
    }
    setFocusedIndex(-1);
  };

  const handleClick = () => setOpen(open ? false : true);
  const textStyle = uppercase ? 'uppercase' : 'normal';

  const customRendererProps = {
    onClick: handleClick,
    iconSize,
    uppercase,
    open,
    item: activeItem,
  };

  return (
    <Relative
      {...props}
      onKeyDown={(e) => navigateDropDown(e, items)}
      ref={wrapperRef}
    >
      <Manager>
        <Reference>
          {({ ref }) =>
            customButtonRenderer ? (
              customButtonRenderer({ ...customRendererProps, ref })
            ) : (
              <IconButton
                labelPosition="left"
                ref={ref}
                onClick={handleClick}
                size={iconSize}
                name={open ? 'arrow-drop-up' : 'arrow-drop-down'}
                px={1}
                textStyle={textStyle}
                label={activeItem.value ? activeItem.label : label}
              />
            )
          }
        </Reference>
        <Popper placement={placement} eventsEnabled>
          {({ ref, style, placement, arrowProps }) =>
            open ? (
              <MenuList
                ref={ref}
                style={style}
                data-placement={placement}
                minWidth={menuMinWidth || undefined}
              >
                {items.map((item, index) => {
                  const selected = activeItem.value === item.value;
                  const focus = index === focusedIndex;
                  const { label, disabled } = item;
                  return item.type === 'divider' ? (
                    <Hr key={index} my={2} />
                  ) : (
                    <ListItem
                      key={index}
                      active={
                        customMenuItemRenderer
                          ? focus
                          : focus || (focusedIndex === -1 && selected)
                      }
                      disabled={disabled}
                      onClick={() =>
                        !disabled && handleToggleAndSelect(item, 'itemClick')
                      }
                    >
                      {customMenuItemRenderer
                        ? customMenuItemRenderer({
                            label,
                            selected,
                            disabled: item.disabled,
                          })
                        : label}
                    </ListItem>
                  );
                })}
              </MenuList>
            ) : null
          }
        </Popper>
      </Manager>
    </Relative>
  );
};

export default withTranslation('common')(DropDownButton);
