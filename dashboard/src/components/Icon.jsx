export default function Icon({ kind }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      aria-hidden="true"
    >
      {kind === "business" ? (
        <>
          <path d="M4 20V10m8 10V4m8 16v-8" />
          <path d="M2 21h20" />
        </>
      ) : kind === "engineering" ? (
        <>
          <rect x="3" y="4" width="18" height="6" rx="2" />
          <rect x="3" y="14" width="18" height="6" rx="2" />
          <path d="M7 7h.01M7 17h.01" />
        </>
      ) : (
        <>
          <circle cx="5" cy="12" r="3" />
          <circle cx="19" cy="5" r="3" />
          <circle cx="19" cy="19" r="3" />
          <path d="m8 11 8-5m-8 7 8 5" />
        </>
      )}
    </svg>
  );
}
