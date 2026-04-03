import type { InputHTMLAttributes } from "react";

type InputProps = InputHTMLAttributes<HTMLInputElement>;

export function Input({ className = "", ...props }: InputProps) {
  return (
    <input
      className={`h-10 rounded-2xl border border-zinc-700 bg-zinc-900 px-3 text-sm text-white outline-none ${className}`}
      {...props}
    />
  );
}
