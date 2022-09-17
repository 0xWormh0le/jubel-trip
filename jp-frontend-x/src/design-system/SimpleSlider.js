import React from 'react';
import {
  Handles,
  Rail,
  Slider as RCSlider,
  Tracks,
} from 'react-compound-slider';
import { get } from 'styled-system';

import Absolute from './Absolute';
import Box from './Box';
import colors, { lighter } from './theme/colors';

const trackColor = (color) => lighter(get(colors, color) || color).toString();

const SliderRail = ({ color, getRailProps }) => (
  <>
    <Absolute
      height={30}
      width={1}
      borderRadius={7}
      hoverable
      transform="translate(0%, -50%)"
      {...getRailProps()}
    />
    <Absolute
      height={10}
      width={1}
      borderRadius={7}
      hoverable
      transform="translate(0%, -50%)"
      css={{ pointerEvents: 'none' }}
      bg={color}
    />
  </>
);

const Handle = ({
  domain: [min, max],
  handle: { id, value, percent },
  disabled,
  color,
  getHandleProps,
}) => (
  <>
    <Absolute
      left={`${percent}%`}
      transform="translate(-50%, -50%)"
      zIndex={50}
      width={20}
      height={30}
      hoverable
      bg="none"
      {...getHandleProps(id)}
    />
    <Absolute
      role="slider"
      aria-valuemin={min}
      aria-valuemax={max}
      aria-valuenow={value}
      left={`${percent}%`}
      transform="translate(-50%, -50%)"
      zIndex={40}
      width={16}
      height={16}
      borderRadius="50%"
      boxShadow="misc.slider.shadow"
      bg={disabled ? 'disabled' : color}
    />
  </>
);

const Track = ({ source, target, getTrackProps, disabled, color }) => (
  <Absolute
    left={`${source.percent}%`}
    width={`${target.percent - source.percent}%`}
    hoverable
    transform="translate(0%,-50%)"
    height={10}
    zIndex={1}
    borderRadius={7}
    bg={disabled ? 'disabled' : color}
    {...getTrackProps()}
  />
);

const SimpleSlider = ({
  min = 0,
  max = 100,
  step = 1,
  value = Math.round((max - min) / 2.0),
  onChange,
  disabled,
  color = 'misc.slider.handle',
  onUpdate,
  ...props
}) => (
  <Box p={2} {...props}>
    <RCSlider
      mode={1}
      domain={[min, max]}
      step={step}
      values={[value]}
      rootStyle={{ position: 'relative', width: '100%', touchAction: 'none' }}
      onChange={onChange ? (values) => onChange(values[0] || 0) : undefined}
      onUpdate={onUpdate ? (values) => onUpdate(values[0] || 0) : undefined}
    >
      <Rail>
        {({ getRailProps }) => (
          <SliderRail
            color="misc.slider.rail"
            disabled={disabled}
            getRailProps={getRailProps}
          />
        )}
      </Rail>
      <Handles>
        {({ handles, getHandleProps }) => (
          <div className="slider-handles">
            {handles.map((handle) => (
              <Handle
                key={handle.id}
                handle={handle}
                domain={[min, max]}
                getHandleProps={getHandleProps}
                disabled={disabled}
                color={color}
              />
            ))}
          </div>
        )}
      </Handles>
      <Tracks right={false}>
        {({ tracks, getTrackProps }) => (
          <div className="slider-tracks">
            {tracks.map(({ id, source, target }) => (
              <Track
                key={id}
                source={source}
                target={target}
                getTrackProps={getTrackProps}
                disabled={disabled}
                color={trackColor(color)}
              />
            ))}
          </div>
        )}
      </Tracks>
    </RCSlider>
  </Box>
);

export default SimpleSlider;
