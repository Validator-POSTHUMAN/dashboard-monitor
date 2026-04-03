import type { HTMLAttributes } from "react";

type BadgeProps = HTMLAttributes<HTMLSpanElement> & {
  variant?: "default" | "outline";
};

export function Badge({ className = "", variant = "default", ...props }: BadgeProps) {
  const variantClass =
    variant === "outline"
      ? "border border-zinc-700 text-zinc-300"
      : "bg-zinc-800 text-zinc-100";

  return (
    <span
      className={`inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium ${variantClass} ${className}`}
      {...props}
    />
  );
}
