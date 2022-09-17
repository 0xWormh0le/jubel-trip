import React from 'react';
import ReactPlayer from 'react-player';

import Relative from './Relative';

const VideoPlayer = ({
  src,
  width = 640,
  height = 360,
  autoPlay = false,
  mx = 'auto',
  ...props
}) => (
  <Relative mx={mx} width={width} height={height} {...props}>
    <ReactPlayer url={src} playing={autoPlay} width={width} height={height} />
  </Relative>
);

export default VideoPlayer;
