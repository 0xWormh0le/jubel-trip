import PropTypes from 'prop-types';
import React, { useEffect, useRef, useState } from 'react';
import ReactDOM from 'react-dom';
import { animated, config, useSpring } from 'react-spring';

import { stopEvent } from '../utils/events';
import { numberString } from '../utils/propTypes';
import system from '../utils/System';
import targetedProps from '../utils/targetedProps';
import Absolute from './Absolute';
import CloseButton from './CloseButton';
import DragHandle from './DragHandle';
import FlexPanel from './FlexPanel';
import Guideline from './Guideline';
import MaxPanel from './MaxPanel';
import ScreenOverlay from './ScreenOverlay';

export const AnimatedMaxPanel = animated(MaxPanel);

export const getValidWidth = (offsetLeft, direction, widths) => {
  let newWidth = Math.min(offsetLeft, widths[widths.length - 1]);
  newWidth = Math.max(newWidth, widths[0]);
  let targetWidth = null;
  if (widths.length <= 2) {
    return { newWidth, targetWidth };
  }
  const foundIndex = widths.findIndex((item) => item > newWidth);
  if (foundIndex > -1) {
    const leftPos = widths[Math.max(foundIndex - 1, 0)];
    const rightPos = widths[foundIndex];
    const meanWidth = (leftPos + rightPos) / 2;
    if (newWidth < meanWidth) {
      newWidth = leftPos;
      if (direction === 'right') {
        targetWidth = rightPos;
      } else {
        targetWidth = null;
      }
    } else {
      newWidth = rightPos;
      if (direction === 'left') {
        targetWidth = leftPos;
      } else {
        targetWidth = null;
      }
    }
  }

  return { newWidth, targetWidth };
};

const getControlPosition = ({ rtl, position, offset }) => {
  const side =
    position === 'opposite' ? (rtl ? 'left' : 'right') : rtl ? 'right' : 'left';
  const otherSide = side === 'left' ? 'right' : 'left';
  return { [side]: offset, [otherSide]: 'auto' };
};

const getCloseProps = ({
  toggleOnHover,
  type = 'arrow',
  position = 'opposite',
  isOpen,
  isVisible,
  rtl,
  offset = '.75rem',
  panelId,
  ...props
}) => {
  let closeProps = {
    wantsCloseButton: isVisible && type !== 'none' && !toggleOnHover,
  };
  if (closeProps.wantsCloseButton) {
    if (['x', 'close', 'clear'].includes(type)) {
      closeProps.name = 'close';
    } else {
      const forward = (isOpen && rtl) || (!isOpen && !rtl);
      closeProps.name = forward ? 'arrow-forward' : 'arrow-back';
    }
    closeProps['data-panel-id'] = panelId;
    closeProps = {
      ...closeProps,
      ...getControlPosition({ rtl, position, offset }),
    };
  }
  return { ...closeProps, ...props };
};

const getHandleProps = ({
  rtl,
  resizable,
  icon = 'drag-vertical',
  position = 'opposite',
  borderRadius = 'sm',
  offset = 0,
  panelId,
  ...props
}) => {
  let handleProps = { wantsHandle: resizable };
  if (handleProps.wantsHandle) {
    handleProps.name = icon;
    if (borderRadius && borderRadius !== 'none') {
      handleProps.borderRadius = `${rtl ? 'right' : 'left'}.${borderRadius}`;
    }
    handleProps['data-panel-id'] = panelId;
    handleProps = {
      ...handleProps,
      ...getControlPosition({ rtl, position, offset }),
    };
  }
  return { ...handleProps, ...props };
};

const getGuidelineProps = ({
  resizing,
  widths,
  maxWidth,
  currentWidth,
  panelId,
  targetX,
  ...props
}) => {
  let guidelineProps = {
    wantsGuideline: resizing && widths.length > 2 && targetX,
  };
  if (guidelineProps.wantsGuideline) {
    guidelineProps.left = targetX; //widths.find((w) => w > currentWidth);
    guidelineProps['data-panel-id'] = panelId;
  }
  return { ...guidelineProps, ...props };
};

const getEventHandlers = ({
  rtl,
  resizable,
  toggleOnHover,
  minWidth,
  maxWidth,
  widths,
  onToggle,
  onResize,
  setState,
  panelId,
  wrapperRef,
}) => {
  const checkPanelId = (event) => {
    const { currentTarget } = event;
    const { dataset } = currentTarget;
    const currentPanelId = `panelId` in dataset && dataset.panelId;
    if (currentPanelId !== panelId) {
      console.debug(`${event.type} event ignored on panel ${panelId}`);
      return false;
    }
    return true;
  };
  const toggle = (event) => {
    if (checkPanelId(event)) {
      // console.debug('toggle', event.type, event.currentTarget);
      setState((prevState) => {
        const { currentWidth: previousWidth } = prevState;
        const currentWidth =
          prevState.currentWidth > minWidth ? minWidth : maxWidth;
        if (onToggle) {
          const isOpen = currentWidth > minWidth;
          const isPartiallyOpen = isOpen && currentWidth < maxWidth;
          onToggle(isOpen, isPartiallyOpen);
        }
        return {
          ...prevState,
          previousWidth,
          currentWidth,
        };
      });
      stopEvent(event);
    }
  };
  const captureMouse = () => {
    document.addEventListener('mouseup', endResize);
    document.addEventListener('mousemove', updateResize);
  };
  const releaseMouse = () => {
    document.removeEventListener('mouseup', endResize);
    document.removeEventListener('mousemove', updateResize);
  };
  // const getWidthDiff = ({ clientX, startX }) =>
  //   (clientX - startX) * (rtl ? -1 : 1);

  const getUpdatedWidths = ({
    clientX,
    startX,
    startW,
    prevDirection,
    lastX,
  }) => {
    const wrapperNode = ReactDOM.findDOMNode(wrapperRef.current);
    const rect = wrapperNode.getBoundingClientRect();
    const startOffset = startW - startX;
    const nextWidth = clientX - rect.x + startOffset;
    let direction = prevDirection;
    if (lastX < clientX) {
      direction = 'right';
    }
    if (lastX > clientX) {
      direction = 'left';
    }
    let { newWidth, targetWidth } = getValidWidth(nextWidth, direction, widths);

    return {
      currentWidth: newWidth,
      targetX: targetWidth ? targetWidth + rect.x : null,
      prevDirection: direction,
    };
  };

  const getUpdatedStateWidths = ({ clientX, prevState }) => {
    const { startX, startW, currentWidth, prevDirection } = prevState;
    const previousWidth = currentWidth;
    const lastX = clientX;
    const updatedWidths = getUpdatedWidths({
      clientX,
      startX,
      startW,
      prevDirection,
      lastX: prevState.lastX,
    });
    return {
      ...prevState,
      ...updatedWidths,
      previousWidth,
      lastX,
    };
  };

  const endResize = (event) => {
    const { clientX } = event;
    setState((prevState) => {
      const updatedWidths = getUpdatedStateWidths({ clientX, prevState });
      const { currentWidth } = updatedWidths;
      if (onToggle) onToggle(currentWidth > minWidth, currentWidth < maxWidth);
      return {
        ...updatedWidths,
        resizing: false,
      };
    });
    releaseMouse();
    stopEvent(event);
  };

  const updateResize = (event) => {
    const { clientX } = event;
    setState((prevState) => {
      const updatedState = getUpdatedStateWidths({ clientX, prevState });
      const { currentWidth } = updatedState;
      if (onResize) onResize(currentWidth > minWidth, currentWidth < maxWidth);
      return updatedState;
    });
    stopEvent(event);
  };

  const startResize = (event) => {
    const { pageX } = event;
    setState((prevState) => ({
      ...prevState,
      resizing: true,
      startX: pageX,
      lastX: pageX,
      startW: prevState.currentWidth,
    }));
    captureMouse();
    stopEvent(event);
  };

  const handlers = {};
  if (toggleOnHover) {
    handlers.onMouseEnter = toggle;
    handlers.onMouseLeave = toggle;
  } else {
    handlers.onClick = toggle;
    handlers.onToggle = toggle;
  }
  if (resizable) {
    handlers.onMouseDownCapture = startResize;
  }

  return handlers;
};

const SlidingPanel = ({
  id,
  open = false,
  animated = true,
  resizable = false,
  rtl = false,
  minWidth = 0,
  maxWidth = 400,
  snapWidths = [],
  toggleOn = 'click',
  onToggle,
  onResize,
  offset = 0,
  zIndex = 201,
  children,
  ...props
}) => {
  const panelId = `slp-${id}`;
  const widths = [minWidth, ...snapWidths, maxWidth];
  const toggleOnHover = toggleOn === 'hover' && !resizable;
  const [state, setState] = useState({
    currentWidth: open ? maxWidth : minWidth,
    previousWidth: 0,
    prevDirection: null,
    resizing: false,
    startX: 0,
    startW: 0,
    lastX: 0,
  });
  const wrapperRef = useRef(null);
  const { currentWidth, previousWidth, resizing, targetX } = state;
  const isOpen = currentWidth > minWidth;
  const isVisible = currentWidth > 0;
  const eventHandlers = getEventHandlers({
    rtl,
    resizable,
    toggleOnHover,
    minWidth,
    maxWidth,
    widths,
    setState,
    onToggle,
    onResize,
    panelId,
    wrapperRef,
  });
  const { wantsCloseButton, ...closeProps } = getCloseProps({
    isOpen,
    isVisible,
    rtl,
    toggleOnHover,
    panelId,
    ...targetedProps(props, 'close'),
  });
  const { wantsHandle, ...handleProps } = getHandleProps({
    rtl,
    resizable,
    ...targetedProps(props, 'handle'),
  });
  const { wantsGuideline, ...guidelineProps } = getGuidelineProps({
    currentWidth,
    maxWidth,
    widths,
    resizing,
    panelId,
    targetX,
    ...targetedProps(props, 'guideline'),
  });
  let { width } = useSpring({
    from: { width: previousWidth },
    width: currentWidth,
    config: config.fast,
    immediate: resizing || !animated,
  });
  props[rtl ? 'right' : 'left'] = offset;
  const childrenArray = Array.isArray(children) ? children : [children];
  const contentChildren = childrenArray.filter(
    (child) => child && child.type.displayName !== 'DependentPanels'
  );
  const dependentPanels = childrenArray.filter(
    (child) => child && child.type.displayName === 'DependentPanels'
  );
  useEffect(() => {
    setState((prevState) => {
      const newWidth = open
        ? prevState.currentWidth <= minWidth
          ? maxWidth
          : prevState.currentWidth
        : minWidth;
      return {
        ...prevState,
        previousWidth:
          newWidth !== prevState.currentWidth
            ? prevState.currentWidth
            : prevState.previousWidth,
        currentWidth: newWidth,
      };
    });
  }, [open, maxWidth, minWidth]);
  return (
    <>
      <AnimatedMaxPanel
        data-panel-id={panelId}
        bg="surface"
        position="absolute"
        top={0}
        bottom={0}
        zIndex={zIndex}
        {...props}
        onToggle={eventHandlers.onToggle}
        onMouseEnter={eventHandlers.onMouseEnter}
        onMouseLeave={eventHandlers.onMouseLeave}
        style={{ width }}
        ref={wrapperRef}
      >
        {dependentPanels.length > 0 &&
          (isOpen || minWidth > 0) &&
          dependentPanels.map((child, index) =>
            React.cloneElement(child, {
              key: child.key || index,
              rtl,
            })
          )}
        <FlexPanel
          flex={1}
          bg="surface"
          boxShadow={1}
          position="relative"
          overflow="hidden"
        >
          {contentChildren.map((child, index) =>
            React.cloneElement(child, {
              key: child.key || index,
            })
          )}
        </FlexPanel>
        {wantsCloseButton && (
          <CloseButton {...closeProps} onClick={eventHandlers.onClick} />
        )}
        {wantsHandle && (
          <DragHandle
            {...handleProps}
            onMouseDownCapture={eventHandlers.onMouseDownCapture}
          />
        )}
      </AnimatedMaxPanel>
      {resizing && <ScreenOverlay zIndex={zIndex - 1} />}
      {wantsGuideline && <Guideline {...guidelineProps} zIndex={zIndex} />}
    </>
  );
};

SlidingPanel.propTypes = {
  children: PropTypes.node,
  rtl: PropTypes.bool,
  maxWidth: numberString,
  minWidth: numberString,
  toggleOn: PropTypes.oneOf(['click', 'hover']),
  animated: PropTypes.bool,
  closeType: PropTypes.oneOf(['x', 'arrow', 'none', 'clear']),
  closePosition: PropTypes.oneOf(['same', 'opposite']),
  open: PropTypes.bool,
  onToggle: PropTypes.func,
  resizable: PropTypes.bool,
  snapWidths: PropTypes.array,
};

const DependentPanels = system(
  'DependentPanels',
  {
    extend: Absolute,
    top: 0,
    width: 0,
    height: '100%',
    zIndex: 0,
  },
  ({ rtl }) => ({
    left: rtl ? 0 : undefined,
    right: rtl ? undefined : 0,
  })
);

SlidingPanel.DependentPanels = DependentPanels;

export default SlidingPanel;
