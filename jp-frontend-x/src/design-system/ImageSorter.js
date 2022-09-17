import React, { useState } from 'react';
import { LazyLoadComponent } from 'react-lazy-load-image-component';
import LightBox from 'react-images';

import { stopEvent } from '../utils/events';
import Flex from './Flex';
import IconButton from './IconButton';
import Relative from './Relative';
import Text from './Text';

const suffixUnit = (value) =>
  typeof value === 'number' ? `${value}px` : value;

const makeImageObjects = (images) =>
  images.map((img) =>
    typeof img === 'string' ? { src: img, thumbnail: img } : img
  );

const ImageSorter = ({
  images, // these are just list of strings or {src:xxx,thumbnail:xxx}
  defaultCaption = '',
  buttonWidth = 32,
  buttonMargin = '20%',
  aspectRatio = 1.618,
  buttonBg = 'transparent',
  buttonColor = 'misc.overlay.color',
  captionBg = 'misc.overlay.bg',
  captionColor = 'misc.overlay.color',
  borderRadius = 'sm',
  width = '100%',
  height,
  children,
  ...props
}) => {
  const imageObjects = makeImageObjects(images);
  const numImages = images.length;
  const [toggle, setToggle] = useState(false);
  const [currentIndex, setCurrentIndex] = useState(0);
  const currentImage = imageObjects[currentIndex];
  const prevIndex = (currentIndex + numImages - 1) % numImages;
  const nextIndex = (currentIndex + numImages + 1) % numImages;
  const f = (ev, g) => {
    stopEvent(ev);
    g();
  };
  if (!currentImage) {
    return null;
  }
  const finalHeight =
    suffixUnit(height) || `calc(${suffixUnit(width)} / ${aspectRatio})`;

  return (
    <LazyLoadComponent>
      <Relative
        {...props}
        width={width}
        style={{
          height: finalHeight,
          cursor: 'pointer',
        }}
        onClick={() => setToggle(!toggle)}
        backgroundImage={`url(${currentImage.src})`}
        color={buttonColor}
        borderRadius={borderRadius}
        backgroundRepeat="no-repeat"
        backgroundSize="cover"
        backgroundPosition="center"
      >
        {children}
        <IconButton
          onClick={(ev) => f(ev, () => setCurrentIndex(prevIndex))}
          position="absolute"
          size={buttonWidth}
          left={0}
          bottom={buttonMargin}
          top={buttonMargin}
          name="arrow-left"
          borderRadius="right.sm"
          bg={buttonBg}
          color={buttonColor}
        />
        <IconButton
          onClick={(ev) => f(ev, () => setCurrentIndex(nextIndex))}
          position="absolute"
          size={buttonWidth}
          bottom={buttonMargin}
          top={buttonMargin}
          right={0}
          name="arrow-right"
          borderRadius="left.sm"
          bg={buttonBg}
          color={buttonColor}
        />
        <Flex
          position="absolute"
          justifyContent="space-between"
          bottom={0}
          left={0}
          right={0}
          fontSize={3}
          bg={captionBg}
          color={captionColor}
          p={2}
        >
          <IconButton
            name="photo-camera"
            size={16}
            label={currentImage.caption || defaultCaption}
            labelMargin={2}
            bg={captionBg}
            color={captionColor}
            hoverable={false}
            truncate
            maxWidth={180}
          />
          <Text nobr>
            {currentIndex + 1} / {numImages}
          </Text>
        </Flex>
        <LightBox
          isOpen={toggle}
          images={imageObjects}
          currentImage={currentIndex}
          onClose={() => setToggle(!toggle)}
          onClickPrev={(ev) => setCurrentIndex(prevIndex)}
          onClickNext={(ev) => setCurrentIndex(nextIndex)}
          onClickImage={(ev) => setCurrentIndex(nextIndex)}
          imageCountSeparator=" / "
          preloadNextImage={true}
          backdropClosesModal
        />
      </Relative>
    </LazyLoadComponent>
  );
};

ImageSorter.displayName = 'ImageSorter';

export default ImageSorter;
