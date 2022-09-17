import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import Text from './Text';

const Link = ({ to, children, href, ...props }) => {
  // If Link component has argument to for passing in link
  if (to)
    return (
      <RouterLink to={to}>
        <Text
          truncate
          hoverable
          decorateOnHover="underline"
          p={2}
          display="inline-block"
          {...props}
        >
          {children}
        </Text>
      </RouterLink>
    );

  // It is intended to be an external link
  if (href)
    return (
      <Text
        as="a"
        truncate
        hoverable
        decorateOnHover="underline"
        href={href}
        {...props}
      >
        {children}
      </Text>
    );

  // Finally, if there is missing link for redirection
  return (
    <Text cursor="pointer" display="inline" {...props}>
      {children}
    </Text>
  );
};

Link.displayName = 'Link';

export default Link;
