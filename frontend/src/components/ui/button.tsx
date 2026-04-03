import type { ButtonHTMLAttributes } from "react";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "default" | "outline";
  size?: "default" | "sm";
};

export function Button({
  className = "",
  variant = "default",
  size = "default",
  type = "button",
  ...props
}: ButtonProps) {
  const variantClass =
    variant === "outline"
      ? "border border-zinc-700 bg-zinc-900 text-white"
      : "bg-zinc-100 text-zinc-900";

  const sizeClass = size === "sm" ? "h-9 px-3 text-sm" : "h-10 px-4 text-sm";

  return (
    <button
      type={type}
      className={`inline-flex items-center justify-center rounded-2xl font-medium transition ${variantClass} ${sizeClass} ${className}`}
      {...props}
    />
  );
}
