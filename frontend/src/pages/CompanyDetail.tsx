import React from 'react';
import { useParams } from 'react-router-dom';

export default function CompanyDetail() {
  const { id } = useParams<{ id: string }>();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-gray-900">Company Details</h1>
        <p className="mt-1 text-sm text-gray-500">
          Company ID: {id}
        </p>
      </div>
      
      <div className="bg-white shadow rounded-lg p-6">
        <p className="text-gray-500">Company detail page - Coming soon</p>
      </div>
    </div>
  );
}