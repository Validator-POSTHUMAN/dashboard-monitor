import type { HTMLAttributes } from "react";

type DivProps = HTMLAttributes<HTMLDivElement>;
type HeadingProps = HTMLAttributes<HTMLHeadingElement>;

export function Card({ className = "", ...props }: DivProps) {
  return <div className={className} {...props} />;
}

export function CardHeader({ className = "", ...props }: DivProps) {
  return <div className={className} {...props} />;
}

export function CardContent({ className = "", ...props }: DivProps) {
  return <div className={className} {...props} />;
}

export function CardTitle({ className = "", ...props }: HeadingProps) {
  return <h3 className={className} {...props} />;
}
