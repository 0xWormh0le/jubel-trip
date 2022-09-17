import {
  Handles,
  Rail,
  Slider as RCSlider,
  Tracks,
} from 'react-compound-slider';
import {
  borderRadius,
  borders,
  bottom,
  boxShadow,
  color,
  fontSize,
  getPx,
  height,
  left,
  maxHeight,
  maxWidth,
  minHeight,
  minWidth,
  position,
  right,
  size,
  space,
  style,
  textAlign,
  themeGet,
  top,
  width,
  zIndex,
} from 'styled-system';

import { isArray } from 'lodash';
import React from 'react';

import Flex from './Flex';
import system from '../utils/System';
import Text from './Text';

const styles = [
  borderRadius,
  borders,
  bottom,
  boxShadow,
  color,
  fontSize,
  height,
  left,
  maxHeight,
  maxWidth,
  minHeight,
  minWidth,
  position,
  right,
  space,
  textAlign,
  top,
  width,
  zIndex,
];

const railsThickness = 3;

/** Map spacing prop to either left or top depending on vertical */
const spacing = (vertical) =>
  style({
    prop: 'spacing',
    key: 'space',
    cssProperty: vertical ? 'left' : 'top',
    transformValue: getPx,
  });

/** Map thickness prop to borderWidth */
const thickness = style({
  prop: 'thickness',
  key: 'borderWidths',
  cssProperty: 'borderWidth',
  transformValue: getPx,
});

export const Label = system(
  'Label',
  {
    extend: Text,
    position: 'absolute',
    fontSize: 2,
    color: 'misc.slider.text',
  },
  styles
);

const HLabel = system(
  'HLabel',
  {
    extend: Label,
    spacing: 'sliderSpacing',
  },
  spacing(false),
  {
    left: '50%',
    transform: 'translateX(-50%)',
  }
);

const VLabel = system(
  'VLabel',
  {
    extend: Label,
    spacing: 0,
  },
  spacing(true),
  {
    top: '50%',
    transform: 'translateY(-50%)',
  }
);

/**********
 * Slider
 **********/
const commonSliderDefaults = {
  extend: RCSlider,
  position: 'relative',
  p: 0,
  mt: 2,
};

/** horizontal slider */
const StyledHSlider = system(
  'StyledHSlider',
  {
    ...commonSliderDefaults,
    width: 1,
    height: 80,
  },
  styles
);

/** vertical slider */
const StyledVSlider = system(
  'StyledVSlider',
  {
    ...commonSliderDefaults,
    height: '100%',
  },
  (p) => ({
    minWidth: `calc(${themeGet(`borderWidths.${railsThickness}`)(
      p
    )} * 2 + ${themeGet('space.sliderSpacing')(p)} * 2)`,
  }),
  styles
);

/** uniform slider api */
export const StyledSlider = ({ children, ...props }) =>
  props.vertical ? (
    <StyledVSlider {...props}>{children}</StyledVSlider>
  ) : (
    <StyledHSlider {...props}>{children}</StyledHSlider>
  );

/*************
 * Rail
 *************/
const commonRailDefaults = {
  bg: 'misc.slider.rail',
  borderRadius: 'sm',
  position: 'absolute',
  spacing: 'sliderSpacing',
  thickness: railsThickness,
};

const commonRailStyles = (p) => ({
  cursor: 'pointer',
  borderStyle: 'solid',
  backgroundColor: themeGet(`colors.${p.disabled ? 'misc.disabled' : p.bg}`)(p),
  borderColor: themeGet(`colors.${p.disabled ? 'misc.disabled' : p.bg}`)(p),
});

/** horizontal rail */
const StyledHRail = system(
  'StyledHRail',
  {
    ...commonRailDefaults,
    width: 1,
    height: '1px',
  },
  styles,
  spacing(false),
  thickness,
  commonRailStyles
);

/** vertical rail */
const StyledVRail = system(
  'StyledVRail',
  {
    ...commonRailDefaults,
    top: 0,
    bottom: 0,
    width: '1px',
  },
  styles,
  spacing(true),
  thickness,
  commonRailStyles
);

/** common rail api */
const StyledRail = ({ vertical, ...props }) =>
  vertical ? (
    <StyledVRail className="rail" {...props} />
  ) : (
    <StyledHRail className="rail" {...props} />
  );

/***************
 * Handles
 ***************/
const commonHandleDefaults = {
  bg: 'misc.slider.handle',
  borderColor: 'misc.slider.handle',
  borderRadius: 'circle',
  boxShadow: 2,
  width: '1rem',
  height: '1rem',
  color: 'misc.slider.text',
  fontSize: 2,
  position: 'absolute',
  zIndex: 2,
  spacing: 'sliderSpacing',
};

const commonHandleStyles = (p) => ({
  cursor: 'pointer',
  backgroundColor: themeGet(`colors.${p.disabled ? 'misc.disabled' : p.bg}`)(p),
  borderColor: themeGet(`colors.${p.disabled ? 'misc.disabled' : p.bg}`)(p),
  boxShadow: themeGet(`shadows.${p.disabled ? 0 : p.boxShadow}`)(p),
});

/**
 * horizonal handle
 */
export const StyledHHandle = system(
  'StyledHHandle',
  commonHandleDefaults,
  styles,
  commonHandleStyles,
  spacing(false),
  size,
  (p) => ({
    top: themeGet('space.sliderSpacing')(p),
    left: `${p.handle.percent}%`,
    transform: `translate(calc(-50% + ${themeGet(
      `borderWidths.${railsThickness}`
    )(p)}), calc(-50% + ${themeGet(`borderWidths.${railsThickness}`)(p)}))`,
  })
);

/**
 * vertical handle
 */
export const StyledVHandle = system(
  'StyledVHandle',
  commonHandleDefaults,
  styles,
  commonHandleStyles,
  spacing(true),
  size,
  (p) => ({
    borderRadius:
      p.type === 'mixer'
        ? themeGet(`radii.xs`)(p)
        : themeGet(`radii.circle`)(p),
    width: p.type === 'mixer' ? themeGet(`space.sliderSpacing`)(p) : p.width,
    height: p.type === 'mixer' ? themeGet(`space.2`)(p) : p.height,
    left: themeGet('space.sliderSpacing')(p),
    top: `${p.handle.percent}%`,
    transform: `translate(calc(-50% + ${themeGet(
      `borderWidths.${railsThickness}`
    )(p)}), calc(-50% + ${themeGet(`borderWidths.${railsThickness}`)(p)}))`,
  })
);

/** common interface to handle */
const StyledHandle = ({ vertical, hideLabel, ...props }) =>
  vertical ? (
    <StyledVHandle {...props}>
      {!hideLabel && (
        <VLabel disabled={props.disabled} spacing={props.spacing}>
          {props.handle.value}
        </VLabel>
      )}
    </StyledVHandle>
  ) : (
    <StyledHHandle {...props}>
      {!hideLabel && (
        <HLabel disabled={props.disabled} spacing={props.spacing}>
          {props.handle.value}
        </HLabel>
      )}
    </StyledHHandle>
  );

/**
 * Tracks
 */

const commonTrackDefaults = {
  borderRadius: 'sm',
  spacing: 'sliderSpacing',
  position: 'absolute',
  thickness: railsThickness,
  zIndex: 1,
};

const ctsBackgroundColor = (props) => {
  if (props.disabled) {
    themeGet(`colors.misc.disabled`);
  } else {
    themeGet(`colors.${props.color}`)(props);
  }
};

const commonTrackStyles = (p) => ({
  cursor: 'pointer',
  borderColor: ctsBackgroundColor(p),
  backgroundColor: ctsBackgroundColor(p),
});

const StyledHTrack = system(
  'StyledHTrack',
  commonTrackDefaults,
  styles,
  spacing(false),
  thickness,
  commonTrackStyles,
  (p) => ({
    borderColor: themeGet(`colors.${p.color}`)(p),
    backgroundColor: themeGet(`colors.${p.color}`)(p),
    borderStyle: 'solid',
    left: `${p.source.percent}%`,
    width: `${p.target.percent - p.source.percent}%`,
  })
);

const StyledVTrack = system(
  'StyledVTrack',
  commonTrackDefaults,
  styles,
  spacing(true),
  thickness,
  commonTrackStyles,
  (p) => ({
    borderColor: themeGet(`colors.${p.color}`)(p),
    backgroundColor: themeGet(`colors.${p.color}`)(p),
    borderStyle: 'solid',
    top: `${p.source.percent}%`,
    height: `${p.target.percent - p.source.percent}%`,
  })
);

const StyledTrack = ({ vertical, ...props }) =>
  vertical ? (
    <StyledVTrack
      source={props.source}
      target={props.target}
      color={props.color}
    />
  ) : (
    <StyledHTrack
      source={props.source}
      target={props.target}
      color={props.color}
    />
  );

const Slider = ({
  name,
  label,
  minWidth,
  height,
  minLabel,
  maxLabel,
  min = 0,
  max = 100,
  step = 1,
  spacing = 'sliderSpacing',
  thickness = 3,
  disabled = false,
  vertical = false,
  rtl = false,
  value,
  onChange = () => {},
  color = 'misc.slider.track',
  labelPosition,
  labelStyle,
  hideLabel = false,
  type,
  borderWidth = 0,
}) => (
  <Flex
    width={1}
    centered
    flexDirection={labelPosition === 'bottom' ? 'column-reverse' : 'column'}
  >
    {label &&
      (vertical ? (
        <Flex style={labelStyle} mt={3}>
          <Text
            dir={rtl ? 'rtl' : 'ltr'}
            color={disabled ? 'misc.disabled' : null}
            fontWeight="bold"
            uppercase
          >
            {label}
          </Text>
        </Flex>
      ) : (
        <Text
          dir={rtl ? 'rtl' : 'ltr'}
          color={disabled ? 'misc.disabled' : null}
          fontWeight="bold"
          uppercase
        >
          {label}
        </Text>
      ))}
    <StyledSlider
      height={height}
      domain={[min, max]}
      step={step}
      vertical={vertical}
      thickness={thickness}
      reversed={(vertical && !rtl) || (!vertical && rtl)}
      disabled={disabled}
      values={isArray(value) ? value : [value]}
      onSlideEnd={(values) => onChange(name, values)}
    >
      {(rtl ? maxLabel : minLabel) && (
        <Label
          bottom={vertical && 0}
          top={!vertical && 0}
          left={0}
          textAlign={vertical ? 'center' : null}
          color={disabled ? 'misc.disabled' : null}
        >
          {rtl ? maxLabel : minLabel}
        </Label>
      )}{' '}
      {(rtl ? minLabel : maxLabel) && (
        <Label
          top={0}
          right={vertical ? null : 0}
          textAlign={vertical ? 'center' : null}
          color={disabled ? 'misc.disabled' : null}
        >
          {rtl ? minLabel : maxLabel}
        </Label>
      )}
      <Rail>
        {({ getRailProps }) =>
          StyledRail({
            vertical,
            disabled,
            ...getRailProps(),
          })
        }
      </Rail>
      <Handles>
        {({ handles, getHandleProps }) => (
          <div className="slider-handles">
            {handles.map((handle) => (
              <StyledHandle
                type={type}
                bg={color}
                key={handle.id}
                hideLabel={hideLabel}
                {...{ vertical, disabled, spacing, handle, color }}
                {...getHandleProps(handle.id)}
              />
            ))}
          </div>
        )}
      </Handles>
      <Tracks left={!(vertical && !rtl)} right={vertical && !rtl}>
        {({ tracks, getTrackProps }) => (
          <div className="slider-tracks">
            {tracks.map(({ id, source, target }) => (
              <StyledTrack
                key={id}
                {...{
                  vertical,
                  disabled,
                  source,
                  target,
                  color,
                }}
                {...getTrackProps(id)}
              />
            ))}
          </div>
        )}
      </Tracks>
    </StyledSlider>
  </Flex>
);

export default Slider;
