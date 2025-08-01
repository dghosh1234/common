Option Explicit

Dim tbl, col
Dim output
Dim colMeta

output = "Inspecting all columns:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            output = output & "  Column: " & col.Code & vbCrLf
            output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
            output = output & "    Length: " & col.Length & "'" & vbCrLf

            ' Check if LengthSemantics is supported
            Set colMeta = col.GetExtendedAttribute("LengthSemantics", False)
            If Not colMeta Is Nothing Then
                output = output & "    LengthSemantics: '" & col.LengthSemantics & "'" & vbCrLf
            Else
                output = output & "    LengthSemantics: [N/A]" & vbCrLf
            End If
        Next
        output = output & vbCrLf
    End If
Next

MsgBox output, vbOKOnly, "DEBUG: Column DataTypes and Semantics"

